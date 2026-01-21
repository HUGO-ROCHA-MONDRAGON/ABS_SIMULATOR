import pandas as pd

FILE = "portfolio.xlsx"
TODAY = pd.Timestamp("2026-01-21")

positions = pd.read_excel(FILE, sheet_name="positions")
trades = pd.read_excel(FILE, sheet_name="trades")

positions.columns = positions.columns.str.strip()
trades.columns = trades.columns.str.strip()

trades["Trade Date"] = pd.to_datetime(trades["Trade Date"], errors="coerce")
if trades["Trade Date"].isna().any():
    bad = trades[trades["Trade Date"].isna()]
    raise ValueError("Unparsable Trade Date. Examples:\n" +
                     bad.head(20).to_string(index=False))

# numerics
for col in ["Quantity", "Price", "Factor"]:
    trades[col] = pd.to_numeric(trades[col], errors="coerce")
    if trades[col].isna().any():
        bad = trades[trades[col].isna()]
        raise ValueError(f"Non-numeric {col}. Examples:\n" +
                         bad[["ISIN", "Trade Date", col]].head(20).to_string(index=False))

positions["ISIN"] = positions["ISIN"].astype(str).str.strip()
trades["ISIN"] = trades["ISIN"].astype(str).str.strip()

# economic value signed
trades["TradeValue"] = trades["Quantity"] * trades["Price"] * trades["Factor"]

# Infer side from sign (your rule)
trades["SideFromSign"] = trades["TradeValue"].apply(lambda x: "BUY" if x > 0 else ("SELL" if x < 0 else "ZERO"))

# Optional: consistency check if you have Side column filled
if "Side" in trades.columns:
    trades["Side"] = trades["Side"].astype(str).str.upper().str.strip()
    ok_side = trades["Side"].isin(["BUY", "SELL"])
    if ok_side.any():
        mism = trades[ok_side & (trades["Side"] != trades["SideFromSign"])]
        print(f"[CHECK] Side vs sign mismatches: {len(mism)}")
        if len(mism):
            print(mism[["ISIN", "Trade Date", "Side", "TradeValue", "SideFromSign"]].head(20).to_string(index=False))

# keep current ISINs
current = positions[positions["Position"] > 0].copy()
current_isins = set(current["ISIN"])
trades_cur = trades[trades["ISIN"].isin(current_isins)].copy()

isins_no_trades = sorted(list(current_isins - set(trades_cur["ISIN"])))
print(f"[CHECK] ISIN in portfolio with no trades: {len(isins_no_trades)}")
if isins_no_trades:
    print("Examples:", isins_no_trades[:20])

def fifo_open_lots_value(trades_isin: pd.DataFrame):
    """
    FIFO using signed TradeValue.
    BUY: TradeValue > 0 adds a lot of size=TradeValue
    SELL: TradeValue < 0 removes size=abs(TradeValue) from oldest lots
    Returns (lots, oversell_value)
    lots: list of [remaining_value, trade_date]
    """
    t = trades_isin.sort_values("Trade Date")
    lots = []

    for _, row in t.iterrows():
        v = float(row["TradeValue"])
        d = row["Trade Date"]

        if v > 0:  # BUY
            lots.append([v, d])

        elif v < 0:  # SELL
            sell_v = abs(v)
            while sell_v > 1e-12 and lots:
                lot_v, lot_d = lots[0]
                if lot_v <= sell_v + 1e-12:
                    sell_v -= lot_v
                    lots.pop(0)
                else:
                    lots[0][0] = lot_v - sell_v
                    sell_v = 0.0

            if sell_v > 1e-6 and not lots:
                return None, sell_v  # sold more than bought (missing history)

        # if v == 0 -> ignore

    return lots, 0.0

def weighted_avg_date(lots):
    total = sum(v for v, _ in lots)
    avg_ts = sum(v * d.timestamp() for v, d in lots) / total
    return pd.to_datetime(avg_ts, unit="s")

rows = []
for isin in sorted(current_isins):
    t = trades_cur[trades_cur["ISIN"] == isin]
    if t.empty:
        rows.append({"ISIN": isin, "avg_acq_date": None, "holding_days": None,
                     "buy_value": 0.0, "sell_value": 0.0, "open_value": None, "oversell_value": None})
        continue

    buy_value = t.loc[t["TradeValue"] > 0, "TradeValue"].sum()
    sell_value = (-t.loc[t["TradeValue"] < 0, "TradeValue"]).sum()  # positive

    lots, oversell = fifo_open_lots_value(t)
    if lots is None:
        rows.append({"ISIN": isin, "avg_acq_date": None, "holding_days": None,
                     "buy_value": buy_value, "sell_value": sell_value, "open_value": 0.0, "oversell_value": oversell})
        continue

    open_value = sum(v for v, _ in lots)
    if open_value <= 1e-9:
        avg_date, holding_days = None, None
    else:
        avg_date = weighted_avg_date(lots)
        holding_days = int((TODAY - avg_date).days)

    rows.append({"ISIN": isin, "avg_acq_date": avg_date, "holding_days": holding_days,
                 "buy_value": buy_value, "sell_value": sell_value, "open_value": open_value, "oversell_value": oversell})

diag = pd.DataFrame(rows).merge(current[["ISIN", "Position"]], on="ISIN", how="left")
diag["weight"] = diag["Position"] / 100.0
diag["weighted_holding_days"] = diag["weight"] * diag["holding_days"]

print("\n[CHECK] Oversells:", int((diag["oversell_value"].fillna(0) > 1e-6).sum()))
print("[CHECK] Missing avg date:", int(diag["avg_acq_date"].isna().sum()))

short = diag.dropna(subset=["holding_days"]).query("holding_days < 30").sort_values("holding_days")
print("\n[CHECK] holding_days < 30:", len(short))
if len(short):
    print(short[["ISIN", "holding_days", "open_value", "Position"]].head(20).to_string(index=False))

avg_days = diag.dropna(subset=["holding_days"])["weighted_holding_days"].sum()
print("\n==============================================")
print(f"Portfolio average holding time: {avg_days:.2f} days")
print(f"≈ {avg_days/365:.2f} years")
print("==============================================")

diag.sort_values("weighted_holding_days", ascending=False).to_excel("holding_time_diagnostics.xlsx", index=False)
print("Exported: holding_time_diagnostics.xlsx")