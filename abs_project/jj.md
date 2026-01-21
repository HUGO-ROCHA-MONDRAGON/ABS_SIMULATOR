import pandas as pd

FILE = "portfolio.xlsx"
TODAY = pd.Timestamp("2026-01-21")
OUTPUT = r"C:\Users\h24826\Desktop\holding_time_simple.xlsx"

# Load
positions = pd.read_excel(FILE, sheet_name="positions")
trades = pd.read_excel(FILE, sheet_name="trades")

# Clean headers
positions.columns = positions.columns.str.strip()
trades.columns = trades.columns.str.strip()

# Standardize
positions["ISIN"] = positions["ISIN"].astype(str).str.strip()
trades["ISIN"] = trades["ISIN"].astype(str).str.strip()

# Dates
trades["Trade Date"] = pd.to_datetime(trades["Trade Date"], errors="coerce")
if trades["Trade Date"].isna().any():
    bad = trades[trades["Trade Date"].isna()]
    raise ValueError("Unparsable Trade Date. Examples:\n" + bad.head(20).to_string(index=False))

# Numerics
for col in ["Quantity", "Price", "Factor"]:
    trades[col] = pd.to_numeric(trades[col], errors="coerce")
    if trades[col].isna().any():
        bad = trades[trades[col].isna()]
        raise ValueError(f"Non-numeric {col}. Examples:\n" + bad[["ISIN", "Trade Date", col]].head(20).to_string(index=False))

# Signed economic value (your convention: sell => negative)
trades["TradeValue"] = trades["Quantity"] * trades["Price"] * trades["Factor"]

# Current ISINs (in portfolio today)
current = positions[positions["Position"] > 0].copy()
current_isins = set(current["ISIN"])

# Keep only trades for current ISINs
tcur = trades[trades["ISIN"].isin(current_isins)].copy()

# Entry date = FIRST BUY date (TradeValue > 0)
entry = (
    tcur[tcur["TradeValue"] > 0]
    .groupby("ISIN", as_index=False)["Trade Date"]
    .min()
    .rename(columns={"Trade Date": "EntryDate"})
)

# Merge
out = current.merge(entry, on="ISIN", how="left")

# Checks: ISIN with no BUY trades
no_buy = out[out["EntryDate"].isna()]
print(f"[CHECK] ISIN in portfolio with no BUY trades found: {len(no_buy)}")
if len(no_buy):
    print(no_buy[["ISIN", "Position"]].head(20).to_string(index=False))

# Holding days
out["HoldingDays"] = (TODAY - out["EntryDate"]).dt.days

# Simple stats (non-weighted)
valid = out.dropna(subset=["HoldingDays"]).copy()
avg_days = valid["HoldingDays"].mean()
median_days = valid["HoldingDays"].median()

print("==============================================")
print(f"Average holding time (non-weighted): {avg_days:.2f} days  (~{avg_days/365:.2f} years)")
print(f"Median holding time:                {median_days:.2f} days  (~{median_days/365:.2f} years)")
print("==============================================")

# Export
summary = pd.DataFrame({
    "Metric": ["Average holding days", "Median holding days"],
    "Value (days)": [avg_days, median_days],
    "Value (years)": [avg_days/365, median_days/365],
})

with pd.ExcelWriter(OUTPUT, engine="xlsxwriter") as writer:
    valid.sort_values("HoldingDays", ascending=False)[["ISIN", "Position", "EntryDate", "HoldingDays"]].to_excel(
        writer, sheet_name="Detail", index=False
    )
    summary.to_excel(writer, sheet_name="Summary", index=False)

print(f"Exported to: {OUTPUT}")