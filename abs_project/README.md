import pandas as pd
from datetime import date

FILE = "portfolio.xlsx"
TODAY = pd.Timestamp("2026-01-21")  # fixé comme tu dis

# --------------------
# Load
# --------------------
positions = pd.read_excel(FILE, sheet_name="positions")
trades = pd.read_excel(FILE, sheet_name="trades")

# Standardize column names (trim spaces)
positions.columns = positions.columns.str.strip()
trades.columns = trades.columns.str.strip()

# Required columns
pos_cols = {"ISIN", "Position"}
trd_cols = {"ISIN", "Trade Date", "Quantity", "Side", "Price", "Factor"}

missing_pos = pos_cols - set(positions.columns)
missing_trd = trd_cols - set(trades.columns)
if missing_pos:
    raise ValueError(f"Missing columns in positions: {missing_pos}")
if missing_trd:
    raise ValueError(f"Missing columns in trades: {missing_trd}")

# Parse dates safely
trades["Trade Date"] = pd.to_datetime(trades["Trade Date"], errors="coerce")

# Hard checks on dates
if trades["Trade Date"].isna().any():
    bad = trades[trades["Trade Date"].isna()]
    raise ValueError(
        "Some Trade Date values could not be parsed. "
        "Check these rows:\n" + bad.head(20).to_string(index=False)
    )

# Normalize side
trades["Side"] = trades["Side"].astype(str).str.upper().str.strip()
bad_side = ~trades["Side"].isin(["BUY", "SELL"])
if bad_side.any():
    raise ValueError("Found Side not in {BUY, SELL}. Examples:\n" +
                     trades.loc[bad_side, ["ISIN", "Trade Date", "Side"]].head(20).to_string(index=False))

# Ensure numerics
for col in ["Quantity", "Price", "Factor"]:
    trades[col] = pd.to_numeric(trades[col], errors="coerce")
    if trades[col].isna().any():
        bad = trades[trades[col].isna()]
        raise ValueError(f"Non-numeric {col} found. Examples:\n" +
                         bad[["ISIN", "Trade Date", col]].head(20).to_string(index=False))

# Economic trade value
trades["TradeValue"] = trades["Quantity"] * trades["Price"] * trades["Factor"]

# Check TradeValue sign (we want positive magnitudes)
if (trades["TradeValue"] <= 0).any():
    bad = trades[trades["TradeValue"] <= 0]
    raise ValueError("Found TradeValue <= 0 (Quantity/Price/Factor issue). Examples:\n" +
                     bad[["ISIN", "Trade Date", "Quantity", "Price", "Factor", "TradeValue"]].head(20).to_string(index=False))

# --------------------
# Filter to current portfolio ISINs
# --------------------
positions["ISIN"] = positions["ISIN"].astype(str).str.strip()
trades["ISIN"] = trades["ISIN"].astype(str).str.strip()

current = positions[positions["Position"] > 0].copy()
current_isins = set(current["ISIN"])

trades_cur = trades[trades["ISIN"].isin(current_isins)].copy()

# Check 1: all ISIN in portfolio have trades?
isins_no_trades = sorted(list(current_isins - set(trades_cur["ISIN"])))
print(f"[CHECK] ISIN in portfolio with no trades in trades sheet: {len(isins_no_trades)}")
if isins_no_trades:
    print("Examples:", isins_no_trades[:20])

# --------------------
# FIFO function (value-based)
# --------------------
def fifo_open_lots(trades_isin: pd.DataFrame):
    """
    Returns remaining open lots under FIFO using TradeValue.
    Each lot: (remaining_value, trade_date)
    """
    t = trades_isin.sort_values("Trade Date")
    lots = []

    for _, row in t.iterrows():
        v = float(row["TradeValue"])
        d = row["Trade Date"]
        side = row["Side"]

        if side == "BUY":
            lots.append([v, d])

        else:  # SELL
            sell_v = v
            while sell_v > 1e-12 and lots:
                lot_v, lot_d = lots[0]
                if lot_v <= sell_v + 1e-12:
                    sell_v -= lot_v
                    lots.pop(0)
                else:
                    lots[0][0] = lot_v - sell_v
                    sell_v = 0.0

            # If we sold more than we had (data issue)
            if sell_v > 1e-6 and not lots:
                return None, sell_v  # oversell amount

    return lots, 0.0

def weighted_avg_date(lots):
    total = sum(v for v, _ in lots)
    avg_ts = sum(v * d.timestamp() for v, d in lots) / total
    return pd.to_datetime(avg_ts, unit="s")

# --------------------
# Build per-ISIN diagnostics
# --------------------
rows = []
for isin in sorted(current_isins):
    t = trades_cur[trades_cur["ISIN"] == isin]
    if t.empty:
        rows.append({
            "ISIN": isin,
            "has_trades": False,
            "buy_value": 0.0,
            "sell_value": 0.0,
            "open_value": None,
            "oversell_value": None,
            "first_buy": None,
            "last_buy": None,
            "avg_acq_date": None,
            "holding_days": None
        })
        continue

    buy_value = t.loc[t["Side"] == "BUY", "TradeValue"].sum()
    sell_value = t.loc[t["Side"] == "SELL", "TradeValue"].sum()

    first_buy = t.loc[t["Side"] == "BUY", "Trade Date"].min()
    last_buy = t.loc[t["Side"] == "BUY", "Trade Date"].max()

    lots, oversell = fifo_open_lots(t)
    if lots is None:
        # Oversold (sold more than bought)
        rows.append({
            "ISIN": isin,
            "has_trades": True,
            "buy_value": buy_value,
            "sell_value": sell_value,
            "open_value": 0.0,
            "oversell_value": oversell,
            "first_buy": first_buy,
            "last_buy": last_buy,
            "avg_acq_date": None,
            "holding_days": None
        })
        continue

    open_value = sum(v for v, _ in lots)
    if open_value <= 1e-9:
        avg_date = None
        holding_days = None
    else:
        avg_date = weighted_avg_date(lots)
        holding_days = int((TODAY - avg_date).days)

    rows.append({
        "ISIN": isin,
        "has_trades": True,
        "buy_value": buy_value,
        "sell_value": sell_value,
        "open_value": open_value,
        "oversell_value": oversell,
        "first_buy": first_buy,
        "last_buy": last_buy,
        "avg_acq_date": avg_date,
        "holding_days": holding_days
    })

diag = pd.DataFrame(rows)

# Merge weights
diag = diag.merge(current[["ISIN", "Position"]], on="ISIN", how="left")
diag["weight"] = diag["Position"] / 100.0
diag["weighted_holding_days"] = diag["weight"] * diag["holding_days"]

# --------------------
# Global checks
# --------------------
print("\n[CHECK] Oversells (sold more than bought):", int((diag["oversell_value"].fillna(0) > 1e-6).sum()))
print("[CHECK] Open value missing/zero:", int(diag["open_value"].fillna(0).le(1e-9).sum()))
print("[CHECK] Missing avg_acq_date:", int(diag["avg_acq_date"].isna().sum()))

# suspiciously short holding
short = diag.dropna(subset=["holding_days"]).query("holding_days < 30").sort_values("holding_days")
print("\n[CHECK] ISIN with holding_days < 30:", len(short))
if len(short):
    print(short[["ISIN", "holding_days", "first_buy", "last_buy", "open_value", "Position"]].head(20).to_string(index=False))

# compute portfolio average holding time (weights from positions)
valid = diag.dropna(subset=["holding_days"])
avg_days = valid["weighted_holding_days"].sum()
print("\n==============================================")
print(f"Portfolio average holding time (weighted by Position%): {avg_days:.2f} days")
print(f"≈ {avg_days/365:.2f} years")
print("==============================================")

# export diagnostics
diag.sort_values("weighted_holding_days", ascending=False).to_excel("holding_time_diagnostics.xlsx", index=False)
print("\nExported: holding_time_diagnostics.xlsx")