import pandas as pd

# ── CONFIG ────────────────────────────────────────────────────────────────────
INPUT_CSV   = "sp500.csv"                        # your raw daily data
WKDAY_OUT   = "inside_outside_weekday_probs.csv"
# ─────────────────────────────────────────────────────────────────────────────

# 1. LOAD & SORT
df = (
    pd.read_csv(
        INPUT_CSV,
        sep=";",
        header=None,
        names=["Date","Open","High","Low","Close","Volume"],
        parse_dates=["Date"],
        date_parser=lambda s: pd.to_datetime(s, format="%Y%m%d"),
    )
    .sort_values("Date")
    .reset_index(drop=True)
)

# 2. FLAG INSIDE / OUTSIDE
df["Prev_High"] = df["High"].shift(1)
df["Prev_Low"]  = df["Low"].shift(1)

# Inside: High ≤ Prev_High AND Low ≥ Prev_Low
df["Inside"] = (df["High"] <= df["Prev_High"]) & (df["Low"] >= df["Prev_Low"])

# Outside: High > Prev_High AND Low < Prev_Low
df["Outside"] = (df["High"] > df["Prev_High"]) & (df["Low"] < df["Prev_Low"])

# Drop first row (no prior day to compare)
df = df.dropna(subset=["Prev_High","Prev_Low"]).copy()

# 3. WEEKDAY PROBABILITIES
df["Weekday"] = df["Date"].dt.day_name()
wk_totals    = df["Weekday"].value_counts().reindex(
                 ["Monday","Tuesday","Wednesday","Thursday","Friday"], fill_value=0)

inside_counts  = df[df["Inside"]]["Weekday"].value_counts().reindex(wk_totals.index, fill_value=0)
outside_counts = df[df["Outside"]]["Weekday"].value_counts().reindex(wk_totals.index, fill_value=0)

inside_pct  = (inside_counts  / wk_totals * 100).round(2)
outside_pct = (outside_counts / wk_totals * 100).round(2)

weekday_summary = pd.DataFrame({
    "Total_Days":    wk_totals,
    "Inside_Days":   inside_counts,
    "Pct_Inside":    inside_pct,
    "Outside_Days":  outside_counts,
    "Pct_Outside":   outside_pct
})
weekday_summary.index.name = "Weekday"
weekday_summary.to_csv(WKDAY_OUT)

print("=== Weekday probabilities ===")
print(weekday_summary.to_string())
print(f"\nSaved → {WKDAY_OUT}\n")

# 4. RUN-LENGTH PROBABILITIES
def run_length_probs(flag_series, max_len=4):
    """
    Given a boolean Series, compute:
      P(run of length N) = (# of windows of length N all True) / (total windows of length N)
    for N=2..max_len.
    """
    results = {}
    s = flag_series.values
    total_days = len(s)
    for N in range(2, max_len+1):
        windows = total_days - N + 1
        if windows <= 0:
            results[N] = float("nan")
            continue
        count = sum(s[i:i+N].all() for i in range(windows))
        results[N] = round(count / windows * 100, 2)
    return results

inside_runs  = run_length_probs(df["Inside"],  max_len=4)
outside_runs = run_length_probs(df["Outside"], max_len=4)

print("=== Run-length probabilities (in % of N-day windows) ===")
print("Length | Inside-streak | Outside-streak")
print("-"*38)
for N in range(2, 5):
    i_pct = f"{inside_runs[N]:5.2f}%" if not pd.isna(inside_runs[N]) else "  N/A"
    o_pct = f"{outside_runs[N]:5.2f}%" if not pd.isna(outside_runs[N]) else "  N/A"
    print(f"  {N:>1}    |    {i_pct:>6}    |    {o_pct:>6}")
