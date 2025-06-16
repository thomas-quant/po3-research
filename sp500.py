import pandas as pd

# ── CONFIG ────────────────────────────────────────────────────────────────────
INPUT_CSV   = "sp500.csv"     # your raw data (semicolon-delimited)
OUTPUT_CSV  = "results.csv"   # where we’ll write the summary
DATE_COL    = "date"
DELIMITER   = ";"
DATE_FORMAT = "%Y%m%d"        # e.g. 20110110
WEEK_START  = "W-MON"         # weeks starting on Monday
# ─────────────────────────────────────────────────────────────────────────────

# 1. LOAD your entire dataset
df = pd.read_csv(
    INPUT_CSV,
    sep=DELIMITER,
    header=None,
    names=[DATE_COL, "Open", "High", "Low", "Close", "Volume"],
    parse_dates=[DATE_COL],
    date_parser=lambda s: pd.to_datetime(s, format=DATE_FORMAT),
)

# 2. TAG with week‐period and weekday name
df[DATE_COL]    = pd.to_datetime(df[DATE_COL])
df["Week"]      = df[DATE_COL].dt.to_period(WEEK_START)
df["Weekday"]   = df[DATE_COL].dt.day_name()

# 3. GROUP BY week and compute metrics
weekly = (
    df
    .groupby("Week", group_keys=False)
    .apply(lambda w: pd.Series({
        "Week_Start" : w[DATE_COL].dt.to_period(WEEK_START).dt.start_time.dt.date.iloc[0],
        "Bull_Bear"  : "Bullish" if w["Close"].iloc[-1] > w["Open"].iloc[0] else "Bearish",
        "High_Day"   : w.loc[w["High"].idxmax(), "Weekday"],
        "Low_Day"    : w.loc[w["Low"].idxmin(),  "Weekday"],
    }))
)

# 4. RESET INDEX (turn 'Week' into a column, if you need it)
weekly.index.name = "Week_Period"
weekly = weekly.reset_index(drop=True)

# 5. SAVE just the columns you asked for
weekly[["Week_Start", "Bull_Bear", "High_Day", "Low_Day"]] \
    .to_csv(OUTPUT_CSV, index=False)

print(f"Results saved to {OUTPUT_CSV}")
