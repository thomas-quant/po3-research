import pandas as pd
import os

# ── CONFIG ────────────────────────────────────────────────────────────────────
INPUT_CSV = "sp500.csv"  # adjust filename as needed
DELIM     = ";"                      # semicolon‐delimited
# ───────────────────────────────────────────────────────────────────────────────

# Check file existence
if not os.path.exists(INPUT_CSV):
    print(f"Input file '{INPUT_CSV}' not found. Please adjust INPUT_CSV to your filename.")
else:
    # 1. LOAD as strings for flexible date parsing
    df = pd.read_csv(
        INPUT_CSV,
        sep=DELIM,
        header=None,
        names=["DateTime", "Open", "High", "Low", "Close", "Volume"],
        dtype={"DateTime": str},
    )

    # 2. PARSE dates flexibly: try datetime with time, else date-only
    dt_parsed = pd.to_datetime(df["DateTime"], format="%Y%m%d %H%M%S", errors="coerce")
    mask = dt_parsed.isna()
    if mask.any():
        dt_parsed_date = pd.to_datetime(df.loc[mask, "DateTime"], format="%Y%m%d", errors="coerce")
        dt_parsed.loc[mask] = dt_parsed_date
    mask2 = dt_parsed.isna()
    if mask2.any():
        dt_parsed_generic = pd.to_datetime(df.loc[mask2, "DateTime"], infer_datetime_format=True, errors="coerce")
        dt_parsed.loc[mask2] = dt_parsed_generic
    df["DateTime"] = dt_parsed
    df = df.dropna(subset=["DateTime"]).copy()

    # 3. Timezone/localization if 1m data in UTC; adjust as needed
    # Detect if times include non-midnight: if any time != 00:00:00, assume intraday UTC
    try:
        has_time = (df["DateTime"].dt.time != pd.to_datetime("00:00").time()).any()
    except Exception:
        has_time = False
    if has_time:
        df["DateTime"] = df["DateTime"].dt.tz_localize("UTC").dt.tz_convert("America/New_York")
    else:
        # Daily data: convert to Eastern at midnight
        df["DateTime"] = df["DateTime"].dt.tz_localize("UTC").dt.tz_convert("America/New_York")

    df = df.set_index("DateTime").sort_index()

    # 4. Add Year period
    df["Year"] = df.index.to_period("Y")

    # 5. Analyze each year: determine bullish, low timestamp
    def analyze_year(year_df):
        open_price = year_df["Open"].iloc[0]
        close_price = year_df["Close"].iloc[-1]
        bullish = close_price > open_price
        low_ts = year_df["Low"].idxmin()
        return pd.Series({
            "Bullish": bullish,
            "Low_TS": low_ts
        })

    yearly = df.groupby("Year").apply(analyze_year)

    # Drop years without data or NaT lows
    yearly = yearly.dropna(subset=["Low_TS"])

    # 6. Compute month and quarter for low TS
    yearly["Low_Month"] = yearly["Low_TS"].dt.month_name()
    yearly["Low_Month_Num"] = yearly["Low_TS"].dt.month
    yearly["Low_Quarter"] = yearly["Low_TS"].dt.quarter

    # 7. Filter bullish years
    bullish_years = yearly[yearly["Bullish"]].copy()
    if bullish_years.empty:
        print("No bullish years found in data.")
    else:
        total_bull_years = len(bullish_years)

        # 8. Distribution of low months in bullish years
        month_counts = bullish_years["Low_Month_Num"].value_counts().sort_index()
        month_dist = (month_counts / total_bull_years * 100).round(2)
        print("📊 Distribution of Low-of-Year Month (Bullish Years Only):")
        for month_num, pct in month_dist.items():
            month_name = pd.to_datetime(f"{month_num}", format="%m").strftime("%B")
            print(f"  {month_name} ({month_num}): {pct:.2f}% of bullish years")

        # 9. Distribution of low quarters in bullish years
        quarter_counts = bullish_years["Low_Quarter"].value_counts().sort_index()
        quarter_dist = (quarter_counts / total_bull_years * 100).round(2)
        print("\n📊 Distribution of Low-of-Year Quarter (Bullish Years Only):")
        for quarter, pct in quarter_dist.items():
            print(f"  Q{quarter}: {pct:.2f}% of bullish years")

        # 10. Save results to CSV
        out_month_df = month_dist.reset_index()
        out_month_df.columns = ["Month_Num", "Pct_Low_Month"]
        out_month_df["Month_Name"] = out_month_df["Month_Num"].apply(lambda x: pd.to_datetime(f"{x}", format="%m").strftime("%B"))

        out_quarter_df = quarter_dist.reset_index()
        out_quarter_df.columns = ["Quarter", "Pct_Low_Quarter"]

        out_month_df.to_csv("low_of_year_month_distribution_bullish.csv", index=False)
        out_quarter_df.to_csv("low_of_year_quarter_distribution_bullish.csv", index=False)
        print("\nSaved → low_of_year_month_distribution_bullish.csv")
        print("Saved → low_of_year_quarter_distribution_bullish.csv")
