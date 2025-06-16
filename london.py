import pandas as pd

# ── CONFIG ────────────────────────────────────────────────────────────────────
INPUT_CSV = "nq.csv"    # your data file in UTC
DATE_COL  = "DateTime"
DELIM     = ";"               # semicolon‐delimited

# session definitions in EST
SESSIONS = {
    "Asia"               : ("19:00", "22:00"),
    "London"             : ("00:00", "05:00"),
    "New York Morning"   : ("09:30", "12:00"),
    "New York Afternoon": ("12:00", "16:00"),
}

def session_of(ts_eastern):
    """Map an Eastern‐tz Timestamp to session name or 'Other'."""
    t = ts_eastern.time()
    for name, (start, end) in SESSIONS.items():
        if pd.to_datetime(start).time() <= t < pd.to_datetime(end).time():
            return name
    return "Other"

# ── 1. LOAD & TIMEZONE ADJUST ────────────────────────────────────────────────
df = pd.read_csv(
    INPUT_CSV,
    sep=DELIM,
    header=None,
    names=[DATE_COL, "Open", "High", "Low", "Close", "Volume"],
    parse_dates=[DATE_COL],
    date_parser=lambda s: pd.to_datetime(s, format="%Y%m%d %H%M%S"),
)

df[DATE_COL] = (
    df[DATE_COL]
      .dt.tz_localize("UTC")
      .dt.tz_convert("America/New_York")
)

df = df.set_index(DATE_COL).sort_index()

# ── 2. WEEKLY AGGREGATION ────────────────────────────────────────────────────
def summarize_week(w):
    open_price  = w["Open"].iloc[0]
    close_price = w["Close"].iloc[-1]
    bullish     = close_price > open_price

    low_ts  = w["Low"].idxmin()
    high_ts = w["High"].idxmax()

    return pd.Series({
        "Bullish"       : bullish,
        "Low_Weekday"   : low_ts.day_name(),
        "Low_Session"   : session_of(low_ts),
        "Low_Hour"      : low_ts.hour,
        "High_Weekday"  : high_ts.day_name(),
        "High_Session"  : session_of(high_ts),
        "High_Hour"     : high_ts.hour,
    })

weekly = df.groupby(df.index.to_period("W-MON"), group_keys=False)\
           .apply(summarize_week)

# ── 3. PRINT DISTRIBUTIONS ──────────────────────────────────────────────────
def print_joint(col_day, col_sess, title, mask):
    ct = pd.crosstab(
        weekly.loc[mask, col_day],
        weekly.loc[mask, col_sess],
        normalize=True
    ).mul(100).round(2)
    print(f"\n=== {title} (in % of these weeks) ===")
    print(ct.reindex(
        ["Monday","Tuesday","Wednesday","Thursday","Friday"], 
        fill_value=0
    ).to_string())

def print_hour(col_hour, title, mask):
    vc = weekly.loc[mask, col_hour].value_counts(normalize=True)\
             .mul(100).sort_index().round(2)
    print(f"\n=== {title} (hour of day, in % of these weeks) ===")
    print(vc.to_string())

for week_type, label in [(True, "Bullish"), (False, "Bearish")]:
    mask = weekly["Bullish"] == week_type

    # Lows
    print_joint("Low_Weekday", "Low_Session", f"Low-of-Week sessions for {label} weeks", mask)
    print_hour ("Low_Hour",    f"Low-of-Week hours for {label} weeks",    mask)

    # Highs
    print_joint("High_Weekday", "High_Session", f"High-of-Week sessions for {label} weeks", mask)
    print_hour ("High_Hour",     f"High-of-Week hours for {label} weeks",     mask)

# ── 4. (Optional) SAVE to CSV ────────────────────────────────────────────────
# You can store the joint distributions in CSVs if desired.
