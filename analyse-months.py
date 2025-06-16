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

    # 4. Add Month period and compute Low_TS per month
    df["Month"] = df.index.to_period("M")
    def analyze_month(month_df):
        open_price = month_df["Open"].iloc[0]
        close_price = month_df["Close"].iloc[-1]
        bullish = close_price > open_price
        if not bullish:
            return pd.Series({
                "Bullish": False,
                "Low_TS": pd.NaT,
            })
        low_ts = month_df["Low"].idxmin()
        return pd.Series({
            "Bullish": True,
            "Low_TS": low_ts,
        })

    monthly = df.groupby("Month").apply(analyze_month)

    # Drop non-bullish months
    bullish_months = monthly[monthly["Bullish"]].dropna(subset=["Low_TS"]).copy()
    if bullish_months.empty:
        print("No bullish months found in data.")
    else:
        # Compute week number and weekday for each low timestamp
        bullish_months["Low_WeekNum"] = (bullish_months["Low_TS"].dt.day - 1) // 7 + 1
        bullish_months["Low_Weekday"] = bullish_months["Low_TS"].dt.day_name()

        # 5. Overall percentage for each week number
        total_bull = len(bullish_months)
        weeknum_counts = bullish_months["Low_WeekNum"].value_counts().sort_index()
        weeknum_dist = (weeknum_counts / total_bull * 100).round(2)

        print("📊 Overall Low-of-Month Week Distribution (Bullish Months Only):")
        for weeknum, pct in weeknum_dist.items():
            print(f"  Week {weeknum}: {pct:.2f}% of bullish months")

        # 6. For each week number, weekday distribution within that week
        records = []
        print("\n📊 Within-Week Low-of-Month Weekday Distribution:")
        for weeknum in weeknum_dist.index:
            subset = bullish_months[bullish_months["Low_WeekNum"] == weeknum]
            count_week = len(subset)
            wd_counts = subset["Low_Weekday"].value_counts().sort_index()
            wd_dist = (wd_counts / count_week * 100).round(2)
            print(f"\nWeek {weeknum} (n={count_week} months):")
            for wd, pct in wd_dist.items():
                print(f"  {wd}: {pct:.2f}%")
            # Store for CSV: include overall week percentage too
            for wd, pct in wd_dist.items():
                records.append({
                    "WeekNumInMonth": weeknum,
                    "OverallWeekPct": weeknum_dist.loc[weeknum],
                    "Low_Weekday": wd,
                    "WeekdayPctWithinWeek": pct
                })

        # 7. Save detailed CSV
        out_df = pd.DataFrame(records)
        out_df.to_csv("monthly_low_week_and_weekday_distribution_bullish.csv", index=False)
        print("\nSaved → monthly_low_week_and_weekday_distribution_bullish.csv")
