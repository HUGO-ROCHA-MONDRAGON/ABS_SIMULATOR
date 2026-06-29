import pandas as pd
import matplotlib.pyplot as plt
import statsmodels.api as sm

file_path = "ton_fichier.xlsx"

# One-sheet Excel
df = pd.read_excel(file_path)

# Clean columns
df.columns = df.columns.str.strip().str.lower()

# Rename for clarity
df = df.rename(columns={
    "date": "Date",
    "mkt_spread": "MarketSpread",
    "gstorm": "DM_Green",
    "gstorm_wal": "WAL_Green",
    "storm": "DM_NonGreen",
    "storm_wal": "WAL_NonGreen"
})

df["Date"] = pd.to_datetime(df["Date"])
df = df.sort_values("Date").dropna()

# Regression: non-green DM explained by its WAL + market spread
X = df[["WAL_NonGreen", "MarketSpread"]]
X = sm.add_constant(X)
y = df["DM_NonGreen"]

model = sm.OLS(y, X).fit()
print(model.summary())

# Reprice non-green at green WAL, same market spread
X_adjusted = pd.DataFrame({
    "const": 1,
    "WAL_NonGreen": df["WAL_Green"],
    "MarketSpread": df["MarketSpread"]
})

df["DM_NonGreen_Adjusted"] = model.predict(X_adjusted)

# Greenium
df["Greenium_Adjusted"] = df["DM_Green"] - df["DM_NonGreen_Adjusted"]

print("\n=== Results ===")
print(f"Average greenium: {df['Greenium_Adjusted'].mean():.2f} bps")
print(f"Median greenium: {df['Greenium_Adjusted'].median():.2f} bps")
print(f"Latest greenium: {df['Greenium_Adjusted'].iloc[-1]:.2f} bps")

# Save output
df.to_excel("greenium_results.xlsx", index=False)

# Chart 1: DM comparison
plt.figure(figsize=(10, 5))
plt.plot(df["Date"], df["DM_Green"], label="Green RMBS DM")
plt.plot(df["Date"], df["DM_NonGreen"], label="Non-green RMBS DM")
plt.plot(df["Date"], df["DM_NonGreen_Adjusted"], label="Non-green adjusted DM")
plt.title("Green vs non-green RMBS DM")
plt.xlabel("Date")
plt.ylabel("DM bps")
plt.legend()
plt.grid(True)
plt.tight_layout()
plt.show()

# Chart 2: adjusted greenium
plt.figure(figsize=(10, 5))
plt.plot(df["Date"], df["Greenium_Adjusted"], label="Adjusted greenium")
plt.axhline(0, linestyle="--")
plt.title("WAL and market-adjusted greenium")
plt.xlabel("Date")
plt.ylabel("Greenium bps")
plt.legend()
plt.grid(True)
plt.tight_layout()
plt.show()

# Chart 3: DM vs WAL
plt.figure(figsize=(7, 5))
plt.scatter(df["WAL_NonGreen"], df["DM_NonGreen"], label="Non-green")
plt.scatter(df["WAL_Green"], df["DM_Green"], label="Green")
plt.title("DM vs WAL")
plt.xlabel("WAL")
plt.ylabel("DM bps")
plt.legend()
plt.grid(True)
plt.tight_layout()
plt.show()