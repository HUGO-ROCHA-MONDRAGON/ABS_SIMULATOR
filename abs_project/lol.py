#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import pandas as pd
from datetime import datetime, timedelta
from pathlib import Path

# ========== CONFIGURATION FIXE ==========

OUTPUT_XLSX = Path(r"C:\Users\Hugo\Desktop\output.xlsx")
JPM_DIR     = Path(r"C:\Users\Hugo\Desktop\JPM")
MARKIT_DIR  = Path(r"C:\Users\Hugo\Desktop\Markit")

RATING_THRESHOLDS = {
    "AAA": 0.50, "AA+": 0.90, "AA": 1.00, "AA-": 1.10,
    "A+": 1.20,  "A": 1.30,   "A-": 1.40,
    "BBB+": 1.60,"BBB":1.80,  "BBB-":2.00,
    "BB+": 2.50, "BB":3.00,   "BB-":3.50,
    "B+": 4.50,  "B":6.00,    "B-":7.50,
    "CCC":10.0,  "CC":12.0,   "C":15.0, "D":20.0
}
DEFAULT_THRESHOLD = 3.0
BATCH_STR = "4:00 pm EST"


# ========== OUTILS ==========

def previous_business_day(ref=None):
    d = ref or datetime.now()
    d -= timedelta(days=1)
    if d.weekday() == 5:  # samedi
        d -= timedelta(days=1)
    elif d.weekday() == 6:  # dimanche
        d -= timedelta(days=2)
    return d

def rating_threshold(r):
    if pd.isna(r):
        return DEFAULT_THRESHOLD
    r = str(r).strip().upper()
    return RATING_THRESHOLDS.get(r, DEFAULT_THRESHOLD)


# ========== CHARGEMENT DES FICHIERS PRIX ==========

def read_jpm_prices(date_dt):
    """Lit colonne A (ISIN) et D (prix) à partir de la ligne 2"""
    path = JPM_DIR / f"JPM_Price_{date_dt:%Y%m%d}.xlsx"
    df = pd.read_excel(path, usecols="A,D", skiprows=1)
    df.columns = ["ISIN", "JPM_Price"]
    df.dropna(subset=["ISIN"], inplace=True)
    df["ISIN"] = df["ISIN"].astype(str).str.strip()
    return df

def read_markit_prices(date_dt):
    """Lit colonne C (ISIN) et J (prix) à partir de la ligne 3"""
    path = MARKIT_DIR / f"BNP_CLO_Pricing_{date_dt:%Y%m%d}.xlsx"
    df = pd.read_excel(path, usecols="C,J", skiprows=2)
    df.columns = ["ISIN", "Markit_Price"]
    df.dropna(subset=["ISIN"], inplace=True)
    df["ISIN"] = df["ISIN"].astype(str).str.strip()
    return df


# ========== CALCUL DES CHALLENGES ==========

def compute_diff(v1, v2, threshold):
    if pd.isna(v1) or pd.isna(v2):
        return pd.NA, False, None
    diff = float(v1) - float(v2)
    flag = abs(diff) > threshold
    comment = None
    if flag:
        sign = "positive" if diff > 0 else "negative"
        comment = f"I have a {sign} difference of more than '{threshold}' in price with another provider"
    return diff, flag, comment


# ========== PIPELINE PRINCIPAL ==========

def main():
    tminus1 = previous_business_day()
    date_str = tminus1.strftime("%Y-%m-%d")

    # 1. Charger output.xlsx
    xls = pd.ExcelFile(OUTPUT_XLSX)
    df = pd.read_excel(xls, sheet_name="table")

    # 2. Charger prix JPM et Markit
    jpm = read_jpm_prices(tminus1)
    markit = read_markit_prices(tminus1)

    # 3. Merge par ISIN
    df["ISIN"] = df["ISIN"].astype(str).str.strip()
    df = df.merge(jpm, on="ISIN", how="left")
    df = df.merge(markit, on="ISIN", how="left")

    # 4. Calcul des differences et flags
    df["Diff_JPM_Markit"], df["Flag_JPM_Markit"], df["Comm_JPM_Markit"] = zip(
        *df.apply(lambda r: compute_diff(r["JPM_Price"], r["Markit_Price"], rating_threshold(r["Rating"])), axis=1)
    )
    df["Diff_JPM_BBG"], df["Flag_JPM_BBG"], df["Comm_JPM_BBG"] = zip(
        *df.apply(lambda r: compute_diff(r["JPM_Price"], r["Price mid Bloomberg"], rating_threshold(r["Rating"])), axis=1)
    )
    df["Diff_Markit_BBG"], df["Flag_Markit_BBG"], df["Comm_Markit_BBG"] = zip(
        *df.apply(lambda r: compute_diff(r["Markit_Price"], r["Price mid Bloomberg"], rating_threshold(r["Rating"])), axis=1)
    )

    # 5. Feuilles de challenges
    markit_chall = df[(df["Flag_JPM_Markit"] == True) | (df["Flag_Markit_BBG"] == True)].copy()
    markit_chall["Value challenged"] = markit_chall["Markit_Price"]
    markit_chall["Date challenged"] = date_str
    markit_chall["Batch"] = BATCH_STR
    markit_chall["Comments"] = markit_chall.apply(
        lambda r: " | ".join([str(c) for c in [r["Comm_JPM_Markit"], r["Comm_Markit_BBG"]] if pd.notna(c)]),
        axis=1
    )

    markit_out = markit_chall[["Security description", "ISIN", "Value challenged", "Date challenged", "Batch", "Comments"]]

    jpm_chall = df[(df["Flag_JPM_Markit"] == True) | (df["Flag_JPM_BBG"] == True)].copy()
    jpm_chall["Value challenged"] = jpm_chall["JPM_Price"]
    jpm_chall["Date challenged"] = date_str
    jpm_chall["Batch"] = BATCH_STR
    jpm_chall["Comments"] = jpm_chall.apply(
        lambda r: " | ".join([str(c) for c in [r["Comm_JPM_Markit"], r["Comm_JPM_BBG"]] if pd.notna(c)]),
        axis=1
    )
    jpm_out = jpm_chall[["Security description", "ISIN", "Value challenged", "Date challenged", "Batch", "Comments"]]

    # 6. Écriture finale
    with pd.ExcelWriter(OUTPUT_XLSX, engine="openpyxl", mode="a", if_sheet_exists="replace") as writer:
        df.to_excel(writer, sheet_name="table", index=False)
        markit_out.to_excel(writer, sheet_name="Challenges_Markit", index=False)
        jpm_out.to_excel(writer, sheet_name="Challenges_JPM", index=False)

    print("✅ Terminé avec succès")
    print(f"Date utilisée : {tminus1:%Y-%m-%d}")
    print(f"Fichiers lus :\n  - {JPM_DIR}\n  - {MARKIT_DIR}")


if __name__ == "__main__":
    main()
