#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Script unique pour:
1) Charger deux fichiers de prix (JPM et Markit) datés au jour ouvré t-1,
2) Faire un "XLOOKUP" par ISIN sur la feuille 'table' du fichier output,
   et écrire Prix JPM (col F) et Prix Markit (col G),
3) Calculer des "challenges" (JPM vs Markit, JPM vs Bloomberg, Markit vs Bloomberg)
   selon des seuils dépendant du rating,
4) Générer deux feuilles récapitulatives: 'Challenges_Markit' et 'Challenges_JPM'.

Usage (chemins par défaut modifiables ci-dessous):
    python main_challenge.py --output_xlsx "/path/to/output.xlsx" --prices_dir "/path/to/prices"
"""

from __future__ import annotations
import argparse
import sys
from pathlib import Path
from datetime import datetime, timedelta
import pandas as pd

# ============ CONFIG ============

# Par défaut, tu peux laisser vide et passer en CLI.
DEFAULT_OUTPUT_XLSX = ""  # ex: r"C:\Users\...\output.xlsx"
DEFAULT_PRICES_DIR  = ""  # ex: r"C:\Users\...\Prices"

# Motifs des fichiers (le YYYYMMDD sera t-1 business day)
JPM_PREFIX    = "JPM_Price_"
MARKIT_PREFIX = "BNP_CLO_Pricing_"  # (= Markit dans ton contexte)
# Extensions acceptées (priorité à xlsx)
POSSIBLE_EXTS = (".xlsx", ".xls", ".csv")

# Noms/alias de colonnes possibles dans les fichiers de prix
ISIN_COL_CANDIDATES  = ["ISIN", "isin", "Isin"]
BID_COL_CANDIDATES   = ["Bid", "BID", "bid", "PX_BID", "Price", "Mid"]  # on prendra ce qu'on trouve, priorité au bid
MID_COL_CANDIDATES   = ["Mid", "MID", "mid", "PX_MID", "PX_Last", "Price"]
DESC_COL_CANDIDATES  = ["Security Description", "Description", "SECURITY_DESCRIPTION", "Name"]

# Feuille "table" du fichier output: attend ces en-têtes (désordre ok, on mappe par nom)
OUTPUT_REQUIRED_HEADERS = {
    "ISIN": ["ISIN", "isin", "Isin"],
    "Rating": ["Rating", "rating", "Credit Rating", "S&P", "Moody's", "Fitch"],
    "WAL": ["WAL", "wal"],
    "BloombergMid": ["Price Bloomberg", "Bloomberg", "PX_MID_BBG", "Mid Bloomberg", "Bloomberg Mid", "E", "Price mid Bloomberg"],
    "SecurityDescription": ["Security description", "Security Description", "Description", "Name", "D"],
}

# Colonnes écrites dans 'table'
COL_PRICE_JPM    = "Price_JPM"     # sera écrite en col F
COL_PRICE_MARKIT = "Price_Markit"  # sera écrite en col G

# Noms des feuilles de synthèse
SHEET_MARKIT = "Challenges_Markit"
SHEET_JPM    = "Challenges_JPM"

# Seuils par rating (exemples, adapte/complète au besoin)
RATING_THRESHOLDS = {
    "AAA": 0.50, "AA+": 0.90, "AA": 1.00, "AA-": 1.10,
    "A+": 1.20,  "A": 1.30,   "A-": 1.40,
    "BBB+": 1.60,"BBB":1.80,  "BBB-":2.00,
    "BB+": 2.50, "BB":3.00,   "BB-":3.50,
    "B+": 4.50,  "B":6.00,    "B-":7.50,
    "CCC":10.0,  "CC":12.0,   "C":15.0, "D":20.0
}
DEFAULT_THRESHOLD = 3.0  # si rating absent/non reconnu

# Heure fixe pour le batch (string)
BATCH_STR = "4:00 pm EST"

# ============ FONCTIONS UTILES ============

def previous_business_day(ref_dt: datetime | None = None) -> datetime:
    """Renvoie la date t-1 ouvré (lundi=0 ... vendredi=4). Simplifié: ignore jours fériés."""
    ref = ref_dt or datetime.now()
    # T-1 naturel
    d = ref - timedelta(days=1)
    # si samedi (5) -> vendredi; si dimanche (6) -> vendredi
    if d.weekday() == 5:   # samedi
        d -= timedelta(days=1)
    elif d.weekday() == 6: # dimanche
        d -= timedelta(days=2)
    return d.replace(hour=0, minute=0, second=0, microsecond=0)

def build_expected_file(prices_dir: Path, prefix: str, date_dt: datetime) -> Path | None:
    """Construit le chemin attendu: prefixYYYYMMDD.ext avec extensions possibles.
       Si non trouvé, tente un 'meilleur match' (dernier fichier commençant par le préfixe)."""
    ymd = date_dt.strftime("%Y%m%d")
    for ext in POSSIBLE_EXTS:
        p = prices_dir / f"{prefix}{ymd}{ext}"
        if p.exists():
            return p
    # fallback: prendre le plus récent qui commence par prefix
    candidates = sorted(prices_dir.glob(f"{prefix}*"), key=lambda x: x.stat().st_mtime, reverse=True)
    for c in candidates:
        if c.suffix.lower() in POSSIBLE_EXTS:
            return c
    return None

def read_prices_file(path: Path) -> pd.DataFrame:
    """Lit un fichier de prix (xlsx/xls/csv) et renvoie un DF standardisé avec colonnes:
       ISIN, Bid, Mid, SecurityDescription (les manquantes seront NaN)."""
    if path.suffix.lower() in [".xlsx", ".xls"]:
        df = pd.read_excel(path)
    elif path.suffix.lower() == ".csv":
        df = pd.read_csv(path)
    else:
        raise ValueError(f"Extension non supportée: {path.suffix}")

    # normalisation des colonnes
    cols = {c: str(c) for c in df.columns}
    df.rename(columns=cols, inplace=True)

    def pick(colnames, prefer="first"):
        for name in colnames:
            if name in df.columns:
                return name
        return None

    isin_col = pick(ISIN_COL_CANDIDATES)
    if not isin_col:
        raise ValueError(f"{path.name}: colonne ISIN introuvable. Colonnes dispo: {list(df.columns)}")

    bid_col = pick(BID_COL_CANDIDATES)
    mid_col = pick(MID_COL_CANDIDATES)
    desc_col = pick(DESC_COL_CANDIDATES)

    out = pd.DataFrame()
    out["ISIN"] = df[isin_col].astype(str).str.strip()

    if bid_col:
        out["Bid"] = pd.to_numeric(df[bid_col], errors="coerce")
    else:
        out["Bid"] = pd.NA

    if mid_col:
        out["Mid"] = pd.to_numeric(df[mid_col], errors="coerce")
    else:
        out["Mid"] = pd.NA

    if desc_col:
        out["SecurityDescription"] = df[desc_col].astype(str)
    else:
        out["SecurityDescription"] = pd.NA

    return out

def map_headers(df: pd.DataFrame, mapping: dict[str, list[str]]) -> dict[str, str]:
    """Trouve pour chaque clé logique la vraie colonne du DF via alias possibles."""
    found = {}
    lower_map = {c.lower(): c for c in df.columns}
    for logical, aliases in mapping.items():
        actual = None
        for a in aliases:
            key = a.lower()
            if key in lower_map:
                actual = lower_map[key]
                break
        if not actual:
            # tolère BloombergMid/SecurityDescription manquants: on ajoutera NaN
            if logical in ("BloombergMid", "SecurityDescription"):
                found[logical] = None
                continue
            raise ValueError(f"En-tête requis manquant pour '{logical}'. Colonnes disponibles: {list(df.columns)}")
        found[logical] = actual
    return found

def rating_threshold(rating: str | float | None) -> float:
    """Retourne le seuil basé sur le rating (insensible à la casse, espaces)."""
    if rating is None or (isinstance(rating, float) and pd.isna(rating)):
        return DEFAULT_THRESHOLD
    r = str(rating).strip().upper()
    return RATING_THRESHOLDS.get(r, DEFAULT_THRESHOLD)

def compute_challenges(row) -> dict[str, dict]:
    """
    Calcule les 3 challenges et retourne un dict avec:
    - 'JPM_vs_Markit': {'flag': bool, 'diff': float, 'comment': str or None, 'who_is_high': 'JPM'/'Markit'/None}
    - 'JPM_vs_BBG'
    - 'Markit_vs_BBG'
    """
    out = {}
    th = rating_threshold(row.get("Rating"))

    jpm = row.get(COL_PRICE_JPM)
    mk  = row.get(COL_PRICE_MARKIT)
    bbg = row.get("BloombergMid")

    def diff_and_flag(v1, v2, name1, name2):
        if pd.isna(v1) or pd.isna(v2):
            return {"flag": False, "diff": pd.NA, "comment": None, "who_is_high": None}
        d = float(v1) - float(v2)
        flag = abs(d) > th
        who = name1 if d > 0 else (name2 if d < 0 else None)
        if flag:
            sign = "positive" if d > 0 else "negative"
            comment = f"I have a {sign} difference of more than '{th}' in price with another provider"
        else:
            comment = None
        return {"flag": flag, "diff": d, "comment": comment, "who_is_high": who}

    out["JPM_vs_Markit"] = diff_and_flag(jpm, mk, "JPM", "Markit")
    out["JPM_vs_BBG"]    = diff_and_flag(jpm, bbg, "JPM", "Bloomberg")
    out["Markit_vs_BBG"] = diff_and_flag(mk,  bbg, "Markit", "Bloomberg")
    return out

# ============ MAIN ============

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--output_xlsx", default=DEFAULT_OUTPUT_XLSX, help="Chemin du fichier Excel 'output' (sera modifié).")
    parser.add_argument("--prices_dir",  default=DEFAULT_PRICES_DIR,  help="Dossier contenant les fichiers de prix.")
    args = parser.parse_args()

    if not args.output_xlsx:
        print("Erreur: --output_xlsx est requis (ou configure DEFAULT_OUTPUT_XLSX en haut du script).", file=sys.stderr)
        sys.exit(1)
    if not args.prices_dir:
        print("Erreur: --prices_dir est requis (ou configure DEFAULT_PRICES_DIR en haut du script).", file=sys.stderr)
        sys.exit(1)

    output_path = Path(args.output_xlsx)
    prices_dir  = Path(args.prices_dir)

    if not output_path.exists():
        print(f"Erreur: fichier output introuvable: {output_path}", file=sys.stderr)
        sys.exit(1)
    if not prices_dir.exists():
        print(f"Erreur: dossier de prix introuvable: {prices_dir}", file=sys.stderr)
        sys.exit(1)

    # 1) Localiser les fichiers de prix à t-1 (ouvré)
    tminus1 = previous_business_day()
    jpm_file    = build_expected_file(prices_dir, JPM_PREFIX, tminus1)
    markit_file = build_expected_file(prices_dir, MARKIT_PREFIX, tminus1)

    if not jpm_file:
        print(f"Erreur: fichier JPM introuvable dans {prices_dir} avec préfixe {JPM_PREFIX}", file=sys.stderr)
        sys.exit(1)
    if not markit_file:
        print(f"Erreur: fichier Markit introuvable dans {prices_dir} avec préfixe {MARKIT_PREFIX}", file=sys.stderr)
        sys.exit(1)

    # 2) Lire les fichiers de prix et standardiser
    jpm_df    = read_prices_file(jpm_file).rename(columns={"Bid": "JPM_Bid", "Mid": "JPM_Mid", "SecurityDescription": "JPM_Desc"})
    markit_df = read_prices_file(markit_file).rename(columns={"Bid": "Markit_Bid", "Mid": "Markit_Mid", "SecurityDescription": "Markit_Desc"})

    # 3) Charger la feuille 'table' du fichier output
    try:
        all_sheets = pd.read_excel(output_path, sheet_name=None)
    except Exception as e:
        print(f"Erreur en lisant {output_path}: {e}", file=sys.stderr)
        sys.exit(1)

    if "table" not in all_sheets:
        print("Erreur: la feuille 'table' est introuvable dans le fichier output.", file=sys.stderr)
        sys.exit(1)

    table = all_sheets["table"].copy()
    # Mapper les headers
    header_map = map_headers(table, OUTPUT_REQUIRED_HEADERS)

    # Créer des colonnes manquantes si besoin
    for logical, colname in header_map.items():
        if colname is None:
            # on ajoute une colonne vide si BloombergMid/SecurityDescription absents
            if logical not in table.columns:
                table[logical] = pd.NA

    # Construire un DF de travail normalisé
    df = pd.DataFrame()
    df["ISIN"] = table[header_map["ISIN"]].astype(str).str.strip()
    df["Rating"] = table[header_map["Rating"]]
    df["WAL"] = pd.to_numeric(table[header_map["WAL"]], errors="coerce")
    df["BloombergMid"] = pd.to_numeric(table[header_map["BloombergMid"]], errors="coerce") if header_map["BloombergMid"] else pd.NA
    df["SecurityDescription"] = table[header_map["SecurityDescription"]] if header_map["SecurityDescription"] else pd.NA

    # 4) Merge type XLOOKUP par ISIN
    df = df.merge(jpm_df[["ISIN", "JPM_Bid"]], on="ISIN", how="left")
    df = df.merge(markit_df[["ISIN", "Markit_Bid"]], on="ISIN", how="left")

    # Écrire les prix dans F et G (on garde la logique par noms; l’ordre final sera géré à l’écriture)
    df[COL_PRICE_JPM]    = pd.to_numeric(df["JPM_Bid"], errors="coerce")
    df[COL_PRICE_MARKIT] = pd.to_numeric(df["Markit_Bid"], errors="coerce")

    # 5) Calcul des challenges
    challenges = df.apply(compute_challenges, axis=1)

    # Extraire et poser des colonnes utiles (diffs/flags)
    def pull(key1, key2):
        return challenges.apply(lambda d: d[key1].get(key2) if isinstance(d, dict) else pd.NA)

    df["Diff_JPM_vs_Markit"]  = pull("JPM_vs_Markit", "diff")
    df["Flag_JPM_vs_Markit"]  = pull("JPM_vs_Markit", "flag")
    df["Comm_JPM_vs_Markit"]  = pull("JPM_vs_Markit", "comment")

    df["Diff_JPM_vs_BBG"]     = pull("JPM_vs_BBG", "diff")
    df["Flag_JPM_vs_BBG"]     = pull("JPM_vs_BBG", "flag")
    df["Comm_JPM_vs_BBG"]     = pull("JPM_vs_BBG", "comment")

    df["Diff_Markit_vs_BBG"]  = pull("Markit_vs_BBG", "diff")
    df["Flag_Markit_vs_BBG"]  = pull("Markit_vs_BBG", "flag")
    df["Comm_Markit_vs_BBG"]  = pull("Markit_vs_BBG", "comment")

    # 6) Générer les feuilles de challenges (Markit & JPM)
    date_challenged = tminus1.date().isoformat()

    # Markit est challengé si:
    # - JPM_vs_Markit flagged (Markit trop haut/bas vs JPM)
    # - Markit_vs_BBG flagged (Markit trop haut/bas vs Bloomberg)
    markit_rows = df[(df["Flag_JPM_vs_Markit"] == True) | (df["Flag_Markit_vs_BBG"] == True)].copy()
    # Valeur challengée = toujours Bid du provider concerné => ici Markit_Bid
    markit_rows["Value challenged"] = markit_rows["Markit_Bid"]
    markit_rows["Date challenged"]  = date_challenged
    markit_rows["Batch"]            = BATCH_STR
    # Commentaire: concatène si deux flags, sinon celui présent
    def mk_comment(r):
        msgs = []
        if pd.notna(r.get("Comm_JPM_vs_Markit")):
            msgs.append(str(r["Comm_JPM_vs_Markit"]))
        if pd.notna(r.get("Comm_Markit_vs_BBG")):
            msgs.append(str(r["Comm_Markit_vs_BBG"]))
        return " | ".join(msgs) if msgs else pd.NA
    markit_rows["Comments"] = markit_rows.apply(mk_comment, axis=1)

    markit_out = markit_rows[[
        "SecurityDescription", "ISIN", "Value challenged", "Date challenged", "Batch", "Comments"
    ]].rename(columns={"SecurityDescription": "Security description"})

    # JPM est challengé si:
    # - JPM_vs_Markit flagged
    # - JPM_vs_BBG flagged
    jpm_rows = df[(df["Flag_JPM_vs_Markit"] == True) | (df["Flag_JPM_vs_BBG"] == True)].copy()
    jpm_rows["Value challenged"] = jpm_rows["JPM_Bid"]
    jpm_rows["Date challenged"]  = date_challenged
    jpm_rows["Batch"]            = BATCH_STR
    def jpm_comment(r):
        msgs = []
        if pd.notna(r.get("Comm_JPM_vs_Markit")):
            msgs.append(str(r["Comm_JPM_vs_Markit"]))
        if pd.notna(r.get("Comm_JPM_vs_BBG")):
            msgs.append(str(r["Comm_JPM_vs_BBG"]))
        return " | ".join(msgs) if msgs else pd.NA
    jpm_rows["Comments"] = jpm_rows.apply(jpm_comment, axis=1)

    jpm_out = jpm_rows[[
        "SecurityDescription", "ISIN", "Value challenged", "Date challenged", "Batch", "Comments"
    ]].rename(columns={"SecurityDescription": "Security description"})

    # 7) Réécrire la feuille 'table' avec les nouvelles colonnes F/G + colonnes de diff/flag (après les colonnes existantes)
    # On repart de la table d’origine pour préserver les colonnes & formats, puis on injecte par ISIN
    base = table.copy()
    # Assure présence des colonnes de sortie
    for col in [COL_PRICE_JPM, COL_PRICE_MARKIT,
                "Diff_JPM_vs_Markit","Flag_JPM_vs_Markit",
                "Diff_JPM_vs_BBG","Flag_JPM_vs_BBG",
                "Diff_Markit_vs_BBG","Flag_Markit_vs_BBG"]:
        if col not in base.columns:
            base[col] = pd.NA

    # Merge par ISIN pour récupérer les nouvelles valeurs
    base = base.merge(
        df[["ISIN", COL_PRICE_JPM, COL_PRICE_MARKIT,
             "Diff_JPM_vs_Markit","Flag_JPM_vs_Markit",
             "Diff_JPM_vs_BBG","Flag_JPM_vs_BBG",
             "Diff_Markit_vs_BBG","Flag_Markit_vs_BBG"]],
        left_on=header_map["ISIN"], right_on="ISIN", how="left", suffixes=("", "_new")
    )

    # Copie des colonnes calculées dans les colonnes cibles
    for col in [COL_PRICE_JPM, COL_PRICE_MARKIT,
                "Diff_JPM_vs_Markit","Flag_JPM_vs_Markit",
                "Diff_JPM_vs_BBG","Flag_JPM_vs_BBG",
                "Diff_Markit_vs_BBG","Flag_Markit_vs_BBG"]:
        src = f"{col}_new"
        if src in base.columns:
            base[col] = base[src]
            base.drop(columns=[src], inplace=True, errors="ignore")

    # Nettoyage colonne ISIN join auxiliaire
    if "ISIN" in base.columns and header_map["ISIN"] != "ISIN":
        base.drop(columns=["ISIN"], inplace=True, errors="ignore")

    # 8) Écriture dans le même fichier Excel (conserve le reste des feuilles)
    all_sheets["table"] = base
    with pd.ExcelWriter(output_path, engine="openpyxl", mode="w") as writer:
        for sname, sdf in all_sheets.items():
            sdf.to_excel(writer, sheet_name=sname, index=False)
        # Ajout/écrasement des feuilles de challenges
        markit_out.to_excel(writer, sheet_name=SHEET_MARKIT, index=False)
        jpm_out.to_excel(writer, sheet_name=SHEET_JPM, index=False)

    print("OK ✅")
    print(f"- Fichier JPM utilisé   : {jpm_file.name}")
    print(f"- Fichier Markit utilisé: {markit_file.name}")
    print(f"- Date challenged       : {date_challenged} ({BATCH_STR})")
    print(f"- Écrit colonnes: {COL_PRICE_JPM} (F), {COL_PRICE_MARKIT} (G) dans 'table'")
    print(f"- Feuilles créées: '{SHEET_MARKIT}', '{SHEET_JPM}'")

if __name__ == "__main__":
    pd.options.display.width = 180
    main()
