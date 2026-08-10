"""
Weekly PO3 Analysis — Monolith
================================
Loads 1-minute parquet data, resamples to any timeframe, and produces
matplotlib charts for weekly high/low timing distributions.

Includes an experiment framework:
    run_experiment(weekly, factor_col="X", target_col="Y")
Plots P(Y | X) for each value of X with the baseline P(Y) overlaid,
so you can visually test whether X predicts the weekly profile.

Usage:
    python analysis.py
"""

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
from pathlib import Path

# ═══════════════════════════════════════════════════════════════════════════════
# CONFIG  ── change these to run different scenarios
# ═══════════════════════════════════════════════════════════════════════════════
SYMBOL      = "ES"                     # label used in chart titles
DATA_PATH   = "data/es_1m.parquet"     # 1-minute OHLCV parquet
RESAMPLE_TO = "1h"                     # target timeframe: "1h", "4h", "1D" …
OUTPUT_DIR  = Path("output")           # where charts are saved
EVENT_OUTPUT_DIR = OUTPUT_DIR / "research_events"
TRAIN_END = pd.Timestamp("2023-12-31", tz="America/New_York")
SPARSE_N = 20
SPARSE_MONTHS = 20                     # monthly sample floor; 192 complete months in ES
BOOTSTRAP_RESAMPLES = 1000
RW_NULL_SIMS = 1000                    # replicas per random-walk null control

# Session definitions (Eastern Time).
# Asia wraps midnight, so we check hour >= 19 separately.
SESSIONS = {
    "Asia"   : (19, 24),   # 19:00 – midnight
    "London" : ( 0,  9),   # 00:00 – 09:30  (approx)
    "NY AM"  : ( 9, 12),   # 09:30 – 12:00
    "NY PM"  : (12, 16),   # 12:00 – 16:00
    "Other"  : (16, 19),   # 16:00 – 19:00
}
SESSION_ORDER = list(SESSIONS.keys())

DAYS = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday"]
COLORS = {"Bullish": "#27ae60", "Bearish": "#e74c3c"}

MONTH_THIRDS = ["Early", "Mid", "Late"]
MONTH_QUINTILES = ["Q1", "Q2", "Q3", "Q4", "Q5"]
WEEK_OF_MONTH_ORDER = ["W1", "W2", "W3", "W4", "W5"]


def module_output_dir(module: str, symbol: str = None, root: Path = None) -> Path:
    """Symbol-scoped output directory for a research module."""
    root = root or EVENT_OUTPUT_DIR
    return root / f"{module}_{(symbol or SYMBOL).lower()}"

# ═══════════════════════════════════════════════════════════════════════════════
# DATA LOADING & RESAMPLING
# ═══════════════════════════════════════════════════════════════════════════════

def _et_index_from_timestamp_columns(df: pd.DataFrame) -> pd.Series:
    """Return ET timestamps from supported source schemas."""
    if "DateTime_ET" in df.columns:
        dt = pd.to_datetime(df["DateTime_ET"])
        if getattr(dt.dt, "tz", None) is None:
            return dt.dt.tz_localize("America/New_York", ambiguous="infer", nonexistent="shift_forward")
        return dt.dt.tz_convert("America/New_York")
    if "DateTime_UTC" in df.columns:
        return pd.to_datetime(df["DateTime_UTC"], utc=True).dt.tz_convert("America/New_York")
    if "datetime_utc" in df.columns:
        return pd.to_datetime(df["datetime_utc"], utc=True).dt.tz_convert("America/New_York")
    raise ValueError("Expected DateTime_ET, DateTime_UTC, or datetime_utc column")


def load_and_resample(path: str, resample_to: str) -> pd.DataFrame:
    """
    Load 1-minute parquet, build ET DatetimeIndex from ET/UTC schema,
    then resample OHLCV to `resample_to` frequency.
    """
    df = pd.read_parquet(path)
    df.index = _et_index_from_timestamp_columns(df)
    df = df.sort_index()

    # Keep only OHLCV
    df = df[["Open", "High", "Low", "Close", "Volume"]].copy()

    if resample_to in ("1T", "1min", "1m"):
        return df

    resampled = (
        df.resample(resample_to, label="left", closed="left")
        .agg({"Open": "first", "High": "max", "Low": "min",
              "Close": "last", "Volume": "sum"})
        .dropna(subset=["Open"])
    )
    return resampled


# ═══════════════════════════════════════════════════════════════════════════════
# SESSION & WEEKDAY HELPERS
# ═══════════════════════════════════════════════════════════════════════════════

def session_of(ts: pd.Timestamp) -> str:
    h = ts.hour
    for name, (lo, hi) in SESSIONS.items():
        if lo <= h < hi:
            return name
    return "Other"


def session_of_index(index: pd.DatetimeIndex) -> np.ndarray:
    """Vectorized `session_of` for an ET DatetimeIndex."""
    hours = np.asarray(index.hour)
    out = np.full(len(hours), "Other", dtype=object)
    for name, (lo, hi) in SESSIONS.items():
        out[(hours >= lo) & (hours < hi)] = name
    return out


def _shift_days_local(value, days):
    """
    Add whole calendar days in local time, then snap to local midnight.

    Timedelta arithmetic on a tz-aware timestamp adds absolute hours, so on the
    two DST-transition Sundays "midnight + 1 day" lands at 23:00 or 01:00. That
    used to split the following Monday into two trading days. Doing the date
    arithmetic naive and re-localizing keeps every session date at midnight.
    """
    tz = getattr(value, "tz", None) or getattr(value, "tzinfo", None)
    naive = value.tz_localize(None) if tz is not None else value
    shifted = naive.normalize() + pd.to_timedelta(days, unit="D")
    return shifted.tz_localize(tz) if tz is not None else shifted


def intraday_trading_day(ts: pd.Timestamp) -> pd.Timestamp:
    """Map ET timestamp to futures session date; 18:00+ belongs to next RTH date."""
    return _shift_days_local(ts, 1 if (ts.hour, ts.minute) >= (18, 0) else 0)


def intraday_trading_day_index(index: pd.DatetimeIndex) -> pd.DatetimeIndex:
    """Vectorized session-date mapping for an ET DatetimeIndex."""
    return _shift_days_local(index, (index.hour >= 18).astype(int))


def trading_weekday(ts: pd.Timestamp) -> str:
    """
    Return the CME session weekday of `ts`.

    The session date rolls at 18:00 ET, so Sunday 18:00 and Monday 09:30 are both
    "Monday", while Monday 19:00 is "Tuesday". Each weekday therefore covers one
    ~23h session (prior 18:00 → 17:00) and the weekday buckets are comparable in
    width. Calendar-day attribution is NOT interchangeable here: it gives Monday
    ~28.5h/week (its own evening plus Sunday's) against Friday's ~17h, which
    inflates Monday as the weekly-low day and deflates Friday.
    """
    return intraday_trading_day(ts).day_name()


def trading_weekday_index(index: pd.DatetimeIndex) -> np.ndarray:
    """Vectorized `trading_weekday` for an ET DatetimeIndex."""
    return np.asarray(intraday_trading_day_index(index).day_name())


def trading_week_monday(ts: pd.Timestamp) -> pd.Timestamp:
    """
    Return the Monday (midnight) of the trading week that `ts` belongs to.

    ES futures reopen Sunday 18:00 ET — that session is the first bar of the
    new Mon–Fri week, so Sunday 18:00+ maps to the NEXT Monday.

    W-MON grouper is WRONG for this: it creates Tue→Mon buckets, so Monday
    always ends up as the last day of the group, artificially inflating it as
    the weekly high/low day.
    """
    day = intraday_trading_day(ts)
    return _shift_days_local(day, -int(day.dayofweek))


def trading_week_monday_index(index: pd.DatetimeIndex) -> pd.DatetimeIndex:
    """Vectorized `trading_week_monday` for an ET DatetimeIndex."""
    days = intraday_trading_day_index(index)
    return _shift_days_local(days, -days.dayofweek)


def trading_month_start(ts: pd.Timestamp) -> pd.Timestamp:
    """
    Return the first calendar day (ET midnight) of the session month of `ts`.

    A session month holds every bar whose session date falls in that calendar
    month, so August 2025 opens at the 18:00 ET Globex print on 31 July and closes
    at the 17:00 print on 29 August. This mirrors the trading week exactly — the
    week opens Sunday 18:00 and is labelled by the following Monday — and the label
    is the calendar anchor, not the first bar, matching `trading_week_monday`.

    Routing through `_shift_days_local` keeps the anchor at local midnight across
    both DST transitions.
    """
    day = intraday_trading_day(ts)
    return _shift_days_local(day, -(day.day - 1))


def trading_month_start_index(index: pd.DatetimeIndex) -> pd.DatetimeIndex:
    """Vectorized `trading_month_start` for an ET DatetimeIndex."""
    days = intraday_trading_day_index(index)
    return _shift_days_local(days, -(np.asarray(days.day) - 1))


def week_of_month(ts: pd.Timestamp) -> str:
    """Calendar week bucket W1–W5 by day of month. W5 spans at most 3 days."""
    return f"W{(ts.day - 1) // 7 + 1}"


def _position_bucket(index: int, n_sessions: int, labels: list) -> str:
    """
    Map a 0-based session index to one of `labels` equal-width position buckets.

    Dividing by `n_sessions` rather than `n_sessions - 1` keeps the buckets equal
    width; the final index is clipped into the last bucket.
    """
    if n_sessions <= 0:
        return labels[0]
    slot = int(index / n_sessions * len(labels))
    return labels[min(max(slot, 0), len(labels) - 1)]


# ═══════════════════════════════════════════════════════════════════════════════
# WEEKLY AGGREGATION
# ═══════════════════════════════════════════════════════════════════════════════

def build_weekly(df: pd.DataFrame) -> pd.DataFrame:
    """
    Group resampled bars into correct Mon–Fri trading weeks and extract:
        Bull_Bear      : "Bullish" / "Bearish"
        Low_Weekday    : trading weekday when weekly low bar formed
        Low_Session    : trading session of the low bar
        Low_Hour       : hour (ET) of the low bar
        High_Weekday   : trading weekday when weekly high bar formed
        High_Session   : trading session of the high bar
        High_Hour      : hour (ET) of the high bar
        Prev_Bull_Bear : previous week's direction (for experiments)
    """
    week_keys = trading_week_monday_index(df.index)

    def summarize(w: pd.DataFrame) -> pd.Series:
        if len(w) < 2:
            return pd.Series(dtype=object)
        low_ts  = w["Low"].idxmin()
        high_ts = w["High"].idxmax()
        return pd.Series({
            "Bull_Bear"   : "Bullish" if w["Close"].iloc[-1] > w["Open"].iloc[0] else "Bearish",
            "Low_Weekday" : trading_weekday(low_ts),
            "Low_Session" : session_of(low_ts),
            "Low_Hour"    : low_ts.hour,
            "High_Weekday": trading_weekday(high_ts),
            "High_Session": session_of(high_ts),
            "High_Hour"   : high_ts.hour,
        })

    weekly = (
        df.groupby(week_keys, group_keys=False)
        .apply(summarize)
        .dropna(how="all")
    )
    weekly["Prev_Bull_Bear"] = weekly["Bull_Bear"].shift(1)
    return weekly




# ═══════════════════════════════════════════════════════════════════════════════
# EVENT DISTRIBUTION RESEARCH
# ═══════════════════════════════════════════════════════════════════════════════

def _close_location_bucket(close: float, low: float, high: float) -> str:
    """Bucket close location inside the developing weekly range."""
    rng = high - low
    if pd.isna(rng) or rng <= 0:
        return "Middle"
    loc = (close - low) / rng
    if loc <= 1 / 3:
        return "Lower"
    if loc >= 2 / 3:
        return "Upper"
    return "Middle"


def _open_return_bucket(ret: float) -> str:
    if ret < -0.001:
        return "Below Open"
    if ret > 0.001:
        return "Above Open"
    return "Near Open"


def _prior_break_bucket(broke_high: bool, broke_low: bool) -> str:
    if broke_high and broke_low:
        return "Both"
    if broke_high:
        return "Prior High Broken"
    if broke_low:
        return "Prior Low Broken"
    return "Neither"


def _range_expansion_buckets(rows: pd.DataFrame) -> pd.Series:
    """
    Bucket developing range against the same slot's TRAIN distribution.

    Thresholds come from train rows only and are then applied to OOS, matching
    every other bucketing helper here. Using the full sample would leak both the
    OOS distribution and future weeks into a feature that is later reported
    split by Train/OOS.
    """
    out = pd.Series("Medium", index=rows.index, dtype=object)
    is_train = rows["Split"].eq("Train")
    for _, idx in rows.groupby("Slot_Index").groups.items():
        vals = rows.loc[idx, "Developing_Range"]
        train_vals = vals[is_train.loc[idx]]
        if len(train_vals) < 3 or train_vals.nunique(dropna=True) < 3:
            continue
        q1, q2 = train_vals.quantile([1 / 3, 2 / 3])
        out.loc[idx[vals <= q1]] = "Low"
        out.loc[idx[(vals > q1) & (vals < q2)]] = "Medium"
        out.loc[idx[vals >= q2]] = "High"
    return out


def build_event_rows(df: pd.DataFrame) -> pd.DataFrame:
    """
    Build one online research row per bar.

    Features use data available at each bar. Final event labels use full-week
    knowledge and are targets, not conditioning features.
    """
    records = []
    prev_week_high = np.nan
    prev_week_low = np.nan

    week_keys = trading_week_monday_index(df.index)
    for week_start, w in df.groupby(week_keys, sort=True):
        if len(w) < 2:
            continue
        w = w.sort_index()
        final_high_ts = w["High"].idxmax()
        final_low_ts = w["Low"].idxmin()
        week_open = float(w["Open"].iloc[0])
        high_so_far = w["High"].cummax()
        low_so_far = w["Low"].cummin()
        developing_range = high_so_far - low_so_far
        broke_prior_high_series = pd.Series(False, index=w.index)
        broke_prior_low_series = pd.Series(False, index=w.index)
        if not pd.isna(prev_week_high):
            broke_prior_high_series = high_so_far >= prev_week_high
        if not pd.isna(prev_week_low):
            broke_prior_low_series = low_so_far <= prev_week_low

        weekdays = trading_weekday_index(w.index)
        sessions = session_of_index(w.index)
        for slot_index, (ts, row) in enumerate(w.iterrows()):
            close = float(row["Close"])
            whsf = float(high_so_far.loc[ts])
            wlsf = float(low_so_far.loc[ts])
            open_return = (close / week_open) - 1 if week_open else np.nan
            broke_high = bool(broke_prior_high_series.loc[ts])
            broke_low = bool(broke_prior_low_series.loc[ts])
            records.append({
                "Week_Start": week_start,
                "Timestamp": ts,
                "Split": "Train" if week_start <= TRAIN_END else "OOS",
                "Weekday": weekdays[slot_index],
                "Session": sessions[slot_index],
                "Hour": ts.hour,
                "Slot_Index": slot_index,
                "Final_High_Timestamp": final_high_ts,
                "Final_Low_Timestamp": final_low_ts,
                "Final_High_Weekday": trading_weekday(final_high_ts),
                "Final_Low_Weekday": trading_weekday(final_low_ts),
                "Final_High_Session": session_of(final_high_ts),
                "Final_Low_Session": session_of(final_low_ts),
                "Final_High_Hour": final_high_ts.hour,
                "Final_Low_Hour": final_low_ts.hour,
                "Is_Final_High_Event": ts == final_high_ts,
                "Is_Final_Low_Event": ts == final_low_ts,
                "High_Already_Formed": ts >= final_high_ts,
                "Low_Already_Formed": ts >= final_low_ts,
                "Week_Open": week_open,
                "Week_High_So_Far": whsf,
                "Week_Low_So_Far": wlsf,
                "Developing_Range": float(developing_range.loc[ts]),
                "Close_Location": (close - wlsf) / (whsf - wlsf) if whsf > wlsf else 0.5,
                "Close_Location_Bucket": _close_location_bucket(close, wlsf, whsf),
                "Return_From_Open": open_return,
                "Open_Return_Bucket": _open_return_bucket(open_return),
                "Broke_Prior_High": broke_high,
                "Broke_Prior_Low": broke_low,
                "Prior_Break_Bucket": _prior_break_bucket(broke_high, broke_low),
            })

        prev_week_high = float(w["High"].max())
        prev_week_low = float(w["Low"].min())

    rows = pd.DataFrame.from_records(records)
    if not rows.empty:
        rows["Range_Expansion_Bucket"] = _range_expansion_buckets(rows)
    return rows


def bootstrap_probability_ci(values, n_resamples: int = BOOTSTRAP_RESAMPLES, seed: int = 42, clusters=None) -> dict:
    """
    Observed true-rate and bootstrap 95% CI in percent.

    `clusters` (normally Week_Start) switches to a cluster bootstrap: whole weeks
    are resampled instead of individual bars. Bars are not independent draws —
    a week contributes ~120 correlated rows and exactly one event — so resampling
    rows reports a CI far narrower than the real uncertainty.
    """
    series = pd.Series(values)
    keep = series.notna()
    arr = series[keep].astype(bool).to_numpy()
    n = len(arr)
    if n == 0:
        return {"n": 0, "n_clusters": 0, "probability": np.nan, "ci_low": np.nan, "ci_high": np.nan}
    probability = float(arr.mean() * 100)
    rng = np.random.default_rng(seed)

    if clusters is None:
        n_clusters = n
        samples = rng.choice(arr, size=(n_resamples, n), replace=True).mean(axis=1) * 100
    else:
        codes = pd.Series(clusters)[keep.to_numpy()].astype("category").cat.codes.to_numpy()
        n_clusters = int(codes.max()) + 1 if len(codes) else 0
        if n_clusters == 0:
            return {"n": n, "n_clusters": 0, "probability": round(probability, 4), "ci_low": np.nan, "ci_high": np.nan}
        sums = np.bincount(codes, weights=arr.astype(float), minlength=n_clusters)
        counts = np.bincount(codes, minlength=n_clusters).astype(float)
        picks = rng.integers(0, n_clusters, size=(n_resamples, n_clusters))
        samples = sums[picks].sum(axis=1) / counts[picks].sum(axis=1) * 100

    lo, hi = np.percentile(samples, [2.5, 97.5])
    return {
        "n": n,
        "n_clusters": n_clusters,
        "probability": round(probability, 4),
        "ci_low": round(float(lo), 4),
        "ci_high": round(float(hi), 4),
    }


def event_timing_distribution(rows: pd.DataFrame, event: str, group_cols: list) -> pd.DataFrame:
    """Distribution of final high/low event slots."""
    event_col = f"Is_Final_{event.capitalize()}_Event"
    events = rows[rows[event_col]].copy()
    if events.empty:
        return pd.DataFrame(columns=group_cols + ["n", "pct"])
    total = len(events)
    out = events.groupby(group_cols, dropna=False).size().reset_index(name="n")
    out["pct"] = (out["n"] / total * 100).round(4)
    return out.sort_values(["pct", "n"], ascending=False).reset_index(drop=True)


def conditional_event_distribution(
    rows: pd.DataFrame,
    event_col: str,
    condition_cols: list,
    n_resamples: int = BOOTSTRAP_RESAMPLES,
    seed: int = 42,
    sparse_n: int = SPARSE_N,
) -> pd.DataFrame:
    """
    Event probability by split and path-state condition with a bootstrap CI.

    CIs and the sparse flag are clustered on `Week_Start` when the column is
    present, so both count independent weeks rather than correlated bars.
    """
    cluster_col = "Week_Start" if "Week_Start" in rows else None
    needed = ["Split", event_col] + condition_cols + ([cluster_col] if cluster_col else [])
    clean = rows[needed].dropna().copy()
    records = []
    for keys, group in clean.groupby(["Split"] + condition_cols, dropna=False):
        if not isinstance(keys, tuple):
            keys = (keys,)
        stats = bootstrap_probability_ci(
            group[event_col],
            n_resamples=n_resamples,
            seed=seed,
            clusters=group[cluster_col] if cluster_col else None,
        )
        rec = {"Split": keys[0]}
        rec.update(dict(zip(condition_cols, keys[1:])))
        rec.update(stats)
        rec["is_sparse"] = stats["n_clusters"] < sparse_n
        records.append(rec)
    return pd.DataFrame(records).sort_values(["Split", "probability"], ascending=[True, False]).reset_index(drop=True)



def remaining_event_distribution(rows: pd.DataFrame, event: str, checkpoint_cols: list, target_cols: list) -> pd.DataFrame:
    """
    Distribution of where the final event lands, conditional on path-state rows
    observed before that event has formed.
    """
    ts_col = f"Final_{event.capitalize()}_Timestamp"
    before_event = rows[rows["Timestamp"] < rows[ts_col]].copy()
    if before_event.empty:
        return pd.DataFrame(columns=checkpoint_cols + target_cols + ["n", "pct"])
    group_cols = checkpoint_cols + target_cols
    counts = before_event.groupby(group_cols, dropna=False).size().reset_index(name="n")
    totals = counts.groupby(checkpoint_cols, dropna=False)["n"].transform("sum")
    counts["pct"] = (counts["n"] / totals * 100).round(4)
    return counts.sort_values(checkpoint_cols + ["pct", "n"], ascending=[True] * len(checkpoint_cols) + [False, False]).reset_index(drop=True)

def survival_curve(rows: pd.DataFrame, event: str) -> pd.DataFrame:
    """Probability the weekly event has not formed yet by slot index."""
    formed_col = f"{event.capitalize()}_Already_Formed"
    out = (
        rows.groupby(["Split", "Slot_Index"], dropna=False)[formed_col]
        .agg(n="size", formed="mean")
        .reset_index()
    )
    out[f"{event}_not_formed_pct"] = ((1 - out["formed"]) * 100).round(4)
    return out.drop(columns=["formed"])


def _write_csv(df: pd.DataFrame, name: str, output_dir: Path = None):
    output_dir = Path(output_dir) if output_dir is not None else EVENT_OUTPUT_DIR
    output_dir.mkdir(parents=True, exist_ok=True)
    path = output_dir / f"{name}.csv"
    df.to_csv(path, index=False)
    print(f"  → {path}")


BARH_MAX_ROWS = 20


def _barh_chart(df: pd.DataFrame, label_cols: list, value_col: str, title: str, filename: str, output_dir: Path = None):
    if df.empty:
        return
    plot = df.copy().head(BARH_MAX_ROWS)
    dropped = len(df) - len(plot)
    labels = plot[label_cols].astype(str).agg(" | ".join, axis=1)
    fig, ax = plt.subplots(figsize=(10, max(4, len(plot) * 0.35)))
    colors = ["#b0b0b0" if bool(v) else "#3498db" for v in plot.get("is_sparse", pd.Series(False, index=plot.index))]
    ax.barh(labels, plot[value_col], color=colors, alpha=0.85)
    ax.invert_yaxis()
    ax.set_xlabel("Probability (%)")
    # Say so when rows are cut, otherwise a truncated chart reads as the whole table.
    ax.set_title(f"{title} — top {len(plot)} of {len(df)} rows" if dropped else title)
    ax.xaxis.set_major_formatter(mticker.PercentFormatter())
    ax.grid(axis="x", alpha=0.25, linestyle="--")
    fig.tight_layout()
    _save(fig, filename, output_dir=output_dir)


def run_event_distribution_research(df: pd.DataFrame, weekly: pd.DataFrame = None, symbol: str = None, output_dir: Path = None) -> pd.DataFrame:
    """Generate research CSV/PNG outputs for online weekly extreme timing."""
    symbol = symbol or SYMBOL
    out_dir = output_dir or module_output_dir("weekly_events", symbol)
    print(f"[Research] {symbol} building online event rows ...")
    rows = build_event_rows(df)
    _write_csv(rows, "event_rows", output_dir=out_dir)

    print("[Research] Event timing distributions ...")
    for event in ["high", "low"]:
        for cols, name in [(["Weekday"], "weekday"), (["Session"], "session"), (["Weekday", "Session"], "weekday_session"), (["Hour"], "hour")]:
            timing = event_timing_distribution(rows, event, cols)
            _write_csv(timing, f"{event}_timing_by_{name}", output_dir=out_dir)
            _barh_chart(timing, cols, "pct", f"{symbol} {event.upper()} timing by {name}", f"{event}_timing_by_{name}", output_dir=out_dir)

    print("[Research] Conditional path-state distributions ...")
    condition_sets = [
        ["Close_Location_Bucket"],
        ["Range_Expansion_Bucket"],
        ["Open_Return_Bucket"],
        ["Prior_Break_Bucket"],
        ["Weekday", "Session", "Close_Location_Bucket"],
    ]
    for event_col, event_name in [("Is_Final_High_Event", "high"), ("Is_Final_Low_Event", "low")]:
        for cond in condition_sets:
            dist = conditional_event_distribution(rows, event_col, cond)
            stem = f"{event_name}_conditional_by_{'_'.join(cond).lower()}"
            _write_csv(dist, stem, output_dir=out_dir)
            _barh_chart(dist[dist["Split"].eq("Train")].sort_values("probability", ascending=False), cond, "probability", f"{symbol} {event_name.upper()} event probability by {' + '.join(cond)} (Train)", stem, output_dir=out_dir)

    print("[Research] Remaining-time distributions ...")
    for event in ["high", "low"]:
        target_cols = [f"Final_{event.capitalize()}_Weekday", f"Final_{event.capitalize()}_Session"]
        for checkpoint_cols, name in [(["Weekday"], "weekday"), (["Weekday", "Session"], "weekday_session"), (["Weekday", "Session", "Close_Location_Bucket"], "weekday_session_close_location")]:
            remaining = remaining_event_distribution(rows, event, checkpoint_cols, target_cols)
            stem = f"{event}_remaining_by_{name}"
            _write_csv(remaining, stem, output_dir=out_dir)
            _barh_chart(remaining, checkpoint_cols + target_cols, "pct", f"{symbol} remaining {event.upper()} target by {' + '.join(checkpoint_cols)}", stem, output_dir=out_dir)

    print("[Research] Survival curves ...")
    for event in ["high", "low"]:
        surv = survival_curve(rows, event)
        _write_csv(surv, f"{event}_survival_by_slot", output_dir=out_dir)
        fig, ax = plt.subplots(figsize=(10, 5))
        for split, sub in surv.groupby("Split"):
            ax.plot(sub["Slot_Index"], sub[f"{event}_not_formed_pct"], marker="o", markersize=2, label=split)
        ax.set_title(f"{symbol} {event.upper()} not formed yet by hourly slot")
        ax.set_xlabel("Hourly slot in trading week")
        ax.set_ylabel("Not formed yet (%)")
        ax.yaxis.set_major_formatter(mticker.PercentFormatter())
        ax.grid(alpha=0.25, linestyle="--")
        ax.legend()
        fig.tight_layout()
        _save(fig, f"{event}_survival_by_slot", output_dir=out_dir)

    strongest = []
    for event_col, event_name in [("Is_Final_High_Event", "HIGH"), ("Is_Final_Low_Event", "LOW")]:
        base = rows[rows["Split"].eq("Train")][event_col].mean() * 100
        dist = conditional_event_distribution(rows, event_col, ["Weekday", "Session", "Close_Location_Bucket"])
        train = dist[(dist["Split"].eq("Train")) & (~dist["is_sparse"])].copy()
        train["excess_vs_baseline"] = train["probability"] - base
        train["Event"] = event_name
        strongest.append(train.sort_values("excess_vs_baseline", ascending=False).head(10))
    if strongest:
        strongest_df = pd.concat(strongest, ignore_index=True)
        _write_csv(strongest_df, "strongest_excess_distributions", output_dir=out_dir)
        print("\n[Research] Strongest non-sparse excess event probabilities (Train):")
        cols = ["Event", "Weekday", "Session", "Close_Location_Bucket", "n", "probability", "ci_low", "ci_high", "excess_vs_baseline"]
        print(strongest_df[cols].to_string(index=False))

    return rows



def load_1m_source(path: str) -> pd.DataFrame:
    """Load source 1-minute data with ET index; supports ET or UTC timestamp columns."""
    df = pd.read_parquet(path)
    df = df.copy()
    df.index = _et_index_from_timestamp_columns(df)
    df = df.sort_index()
    return df[["Open", "High", "Low", "Close", "Volume"]].copy()


def _is_weekly_open_revisit_eligible(ts: pd.Timestamp) -> bool:
    """
    Revisit counting starts at Monday 09:30 ET.

    The Sunday-evening reopen and the Monday overnight session are excluded so a
    revisit is not scored against the thin hours that set the weekly open itself.
    Everything from Monday 09:30 through the Friday close counts.
    """
    if ts.dayofweek == 0:
        return (ts.hour, ts.minute) >= (9, 30)
    return 1 <= ts.dayofweek <= 4


def _weekly_open_revisit_eligible_index(index: pd.DatetimeIndex) -> np.ndarray:
    """Vectorized `_is_weekly_open_revisit_eligible`."""
    dow = np.asarray(index.dayofweek)
    minutes = np.asarray(index.hour) * 60 + np.asarray(index.minute)
    return np.where(dow == 0, minutes >= 9 * 60 + 30, (dow >= 1) & (dow <= 4))


def build_weekly_open_revisit_rows(df_1m: pd.DataFrame, weekly: pd.DataFrame = None) -> pd.DataFrame:
    """One row per week summarizing weekly-open revisits/crosses from 1m bars."""
    records = []
    week_keys = trading_week_monday_index(df_1m.index)
    weekly_lookup = weekly if weekly is not None else build_weekly(df_1m.resample("1h", label="left", closed="left").agg({"Open":"first","High":"max","Low":"min","Close":"last","Volume":"sum"}).dropna(subset=["Open"]))

    for week_start, w in df_1m.groupby(week_keys, sort=True):
        if len(w) < 2:
            continue
        w = w.sort_index().copy()
        weekly_open = float(w["Open"].iloc[0])
        eligible = _weekly_open_revisit_eligible_index(w.index)
        crosses = (w["Low"] <= weekly_open) & (w["High"] >= weekly_open) & eligible
        cross_rows = w[crosses]
        rec = {
            "Week_Start": week_start,
            "Split": "Train" if week_start <= TRAIN_END else "OOS",
            "Weekly_Open": weekly_open,
            "Revisited_Weekly_Open": bool(crosses.any()),
            "Total_Cross_Bars": int(crosses.sum()),
        }
        weekdays = trading_weekday_index(w.index)
        for day in DAYS:
            day_mask = pd.Series(weekdays == day, index=w.index)
            rec[f"{day}_Revisited"] = bool((crosses & day_mask).any())
            rec[f"{day}_Cross_Bars"] = int((crosses & day_mask).sum())

        if not cross_rows.empty:
            first_ts = cross_rows.index[0]
            rec.update({
                "First_Revisit_Timestamp": first_ts,
                "First_Revisit_Weekday": trading_weekday(first_ts),
                "First_Revisit_Session": session_of(first_ts),
                "First_Revisit_Hour": first_ts.hour,
                "First_Revisit_Slot_Index": int(w.index.get_loc(first_ts)),
                "Close_Above_Weekly_Open": bool(w["Close"].iloc[-1] > weekly_open),
            })
        else:
            rec.update({
                "First_Revisit_Timestamp": pd.NaT,
                "First_Revisit_Weekday": np.nan,
                "First_Revisit_Session": np.nan,
                "First_Revisit_Hour": np.nan,
                "First_Revisit_Slot_Index": np.nan,
                "Close_Above_Weekly_Open": bool(w["Close"].iloc[-1] > weekly_open),
            })

        if week_start in weekly_lookup.index:
            for col in ["Bull_Bear", "Low_Weekday", "High_Weekday", "Low_Session", "High_Session"]:
                rec[col] = weekly_lookup.loc[week_start, col]
        records.append(rec)

    rows = pd.DataFrame.from_records(records)
    if not rows.empty:
        rows = apply_revisit_percentile_buckets(rows)
    return rows


def apply_revisit_percentile_buckets(rows: pd.DataFrame) -> pd.DataFrame:
    """Use train first-revisit timing quantiles to label train and OOS rows."""
    out = rows.copy()
    valid_train = out[(out["Split"].eq("Train")) & (out["First_Revisit_Slot_Index"].notna())]["First_Revisit_Slot_Index"]
    if valid_train.empty:
        out["First_Revisit_Timing_Bucket"] = np.nan
        out["First_Revisit_Is_Late_P80"] = False
        out["First_Revisit_Is_Late_P90"] = False
        return out
    q25, q75, q80, q90 = valid_train.quantile([0.25, 0.75, 0.80, 0.90])

    def bucket(v):
        if pd.isna(v):
            return "No Revisit"
        if v <= q25:
            return "Early 0-25%"
        if v <= q75:
            return "Normal 25-75%"
        if v < q90:
            return "Late 75-90%"
        return "Extreme Late 90-100%"

    out["First_Revisit_Timing_Bucket"] = out["First_Revisit_Slot_Index"].map(bucket)
    out["First_Revisit_Is_Late_P80"] = out["First_Revisit_Slot_Index"] >= q80
    out["First_Revisit_Is_Late_P90"] = out["First_Revisit_Slot_Index"] >= q90
    out.loc[out["First_Revisit_Slot_Index"].isna(), ["First_Revisit_Is_Late_P80", "First_Revisit_Is_Late_P90"]] = False
    return out


def weekly_open_revisit_day_distribution(rows: pd.DataFrame) -> pd.DataFrame:
    records = []
    for split, sub in rows.groupby("Split", dropna=False):
        weeks = len(sub)
        for day in DAYS:
            hits = int(sub[f"{day}_Revisited"].sum())
            records.append({
                "Split": split,
                "Weekday": day,
                "weeks": weeks,
                "revisit_weeks": hits,
                "revisit_pct": round(hits / weeks * 100, 4) if weeks else np.nan,
                "avg_cross_bars": round(float(sub[f"{day}_Cross_Bars"].mean()), 4) if weeks else np.nan,
                "median_cross_bars": round(float(sub[f"{day}_Cross_Bars"].median()), 4) if weeks else np.nan,
            })
    return pd.DataFrame(records)


def weekly_open_revisit_outcomes(rows: pd.DataFrame) -> pd.DataFrame:
    cols = ["First_Revisit_Timing_Bucket", "Bull_Bear", "High_Weekday", "Low_Weekday", "Close_Above_Weekly_Open"]
    clean = rows[rows["Revisited_Weekly_Open"]].dropna(subset=["First_Revisit_Timing_Bucket"])
    records = []
    for keys, group in clean.groupby(["Split", "First_Revisit_Timing_Bucket"], dropna=False):
        split, bucket = keys
        n = len(group)
        records.append({
            "Split": split,
            "First_Revisit_Timing_Bucket": bucket,
            "n": n,
            "bullish_pct": round((group["Bull_Bear"].eq("Bullish").mean() * 100), 4) if "Bull_Bear" in group else np.nan,
            "close_above_weekly_open_pct": round((group["Close_Above_Weekly_Open"].mean() * 100), 4),
            "high_friday_pct": round((group["High_Weekday"].eq("Friday").mean() * 100), 4) if "High_Weekday" in group else np.nan,
            "low_friday_pct": round((group["Low_Weekday"].eq("Friday").mean() * 100), 4) if "Low_Weekday" in group else np.nan,
            "high_monday_pct": round((group["High_Weekday"].eq("Monday").mean() * 100), 4) if "High_Weekday" in group else np.nan,
            "low_monday_pct": round((group["Low_Weekday"].eq("Monday").mean() * 100), 4) if "Low_Weekday" in group else np.nan,
        })
    return pd.DataFrame(records)


def run_weekly_open_revisit_research(path: str = DATA_PATH, weekly: pd.DataFrame = None, symbol: str = None, output_dir: Path = None) -> pd.DataFrame:
    """Generate weekly-open revisit research from source 1m data."""
    symbol = symbol or SYMBOL
    out_dir = output_dir or module_output_dir("weekly_open_revisit", symbol)
    print(f"[Research] {symbol} weekly-open revisits from 1m source ...")
    df_1m = load_1m_source(path)
    rows = build_weekly_open_revisit_rows(df_1m, weekly)
    _write_csv(rows, "weekly_open_revisit_rows", output_dir=out_dir)

    day_dist = weekly_open_revisit_day_distribution(rows)
    _write_csv(day_dist, "weekly_open_revisit_by_day", output_dir=out_dir)
    _barh_chart(day_dist[day_dist["Split"].eq("Train")].sort_values("revisit_pct", ascending=False), ["Weekday"], "revisit_pct", f"{symbol} weekly open revisit by day (Train)", "weekly_open_revisit_by_day", output_dir=out_dir)

    first_dist = rows[rows["Revisited_Weekly_Open"]].groupby(["Split", "First_Revisit_Timing_Bucket"], dropna=False).size().reset_index(name="n")
    first_dist["pct"] = first_dist.groupby("Split")["n"].transform(lambda x: (x / x.sum() * 100).round(4))
    _write_csv(first_dist, "weekly_open_first_revisit_timing_buckets", output_dir=out_dir)
    _barh_chart(first_dist[first_dist["Split"].eq("Train")].sort_values("pct", ascending=False), ["First_Revisit_Timing_Bucket"], "pct", f"{symbol} first weekly-open revisit timing buckets (Train)", "weekly_open_first_revisit_timing_buckets", output_dir=out_dir)

    outcomes = weekly_open_revisit_outcomes(rows)
    _write_csv(outcomes, "weekly_open_revisit_outcomes_by_timing_bucket", output_dir=out_dir)
    print("\n[Research] Weekly-open revisit outcomes by timing bucket:")
    print(outcomes.to_string(index=False))
    return rows


# ═══════════════════════════════════════════════════════════════════════════════
# INTRADAY KEY-LEVEL REVISIT RESEARCH
# ═══════════════════════════════════════════════════════════════════════════════

KEY_LEVEL_TIMES = {
    "Globex_Open": (18, 0),
    "NY_Midnight_Open": (0, 0),
    "NY_0930_Open": (9, 30),
    "NY_1300_Open": (13, 0),
}


def _next_session_bounds(sessions: np.ndarray, start_pos: int) -> tuple[int, int] | None:
    """
    Positions [first, last] of the next session block after `start_pos`.

    The next session is the first *contiguous* run whose label differs from the
    label at `start_pos`. Sessions are not unique within a futures day — "Other"
    covers both 16:00–19:00 and the 18:00 open — so the block must be bounded by
    the first label change after it, not by "last bar with a different label"
    (which would silently return the day's final bar).
    """
    current = sessions[start_pos]
    n = len(sessions)
    first = start_pos
    while first < n and sessions[first] == current:
        first += 1
    if first >= n:
        return None
    nxt = sessions[first]
    last = first
    while last + 1 < n and sessions[last + 1] == nxt:
        last += 1
    return first, last


def _next_session_return(day: pd.DataFrame, first_ts: pd.Timestamp) -> float:
    """Return from the revisit close to the close of the next session in the same trading day."""
    positions = day.index.searchsorted(first_ts, side="left")
    start_pos = int(positions)
    if start_pos >= len(day):
        return np.nan
    closes = day["Close"].to_numpy()
    start_close = float(closes[start_pos])
    bounds = _next_session_bounds(session_of_index(day.index), start_pos)
    if bounds is None or start_close == 0:
        return np.nan
    return float((closes[bounds[1]] / start_close - 1) * 100)


def build_intraday_key_level_rows(df_1m: pd.DataFrame) -> pd.DataFrame:
    """One row per trading day × key level with revisit/path/extreme context."""
    records = []
    day_keys = intraday_trading_day_index(df_1m.index)
    for trading_day, day in df_1m.groupby(day_keys, sort=True):
        if len(day) < 2:
            continue
        day = day.sort_index()
        day_open = float(day["Open"].iloc[0])
        day_close = float(day["Close"].iloc[-1])
        day_high_ts = day["High"].idxmax()
        day_low_ts = day["Low"].idxmin()
        day_range = float(day["High"].max() - day["Low"].min())

        for level_name, (hour, minute) in KEY_LEVEL_TIMES.items():
            level_bars = day[(day.index.hour == hour) & (day.index.minute == minute)]
            if level_bars.empty:
                continue
            level_ts = level_bars.index[0]
            level_value = float(level_bars.iloc[0]["Open"])
            watch = day[day.index > level_ts].copy()
            touches = watch[(watch["Low"] <= level_value) & (watch["High"] >= level_value)]
            first_ts = touches.index[0] if not touches.empty else pd.NaT

            rec = {
                "Trading_Day": trading_day,
                "Split": "Train" if trading_day <= TRAIN_END else "OOS",
                "Level_Name": level_name,
                "Level_Timestamp": level_ts,
                "Level_Value": level_value,
                "Revisited": bool(not touches.empty),
                "Touch_Bars_Total": int(len(touches)),
                "Day_Open": day_open,
                "Day_Close": day_close,
                "Day_Bullish": bool(day_close > day_open),
                "Day_Close_Above_Level": bool(day_close > level_value),
                "Day_Return_Pct": (day_close / day_open - 1) * 100 if day_open else np.nan,
                "Day_Range": day_range,
                "Day_High_Timestamp": day_high_ts,
                "Day_Low_Timestamp": day_low_ts,
                "Day_High_Session": session_of(day_high_ts),
                "Day_Low_Session": session_of(day_low_ts),
            }

            touch_sessions = session_of_index(touches.index)
            for session in SESSION_ORDER:
                n_touches = int((touch_sessions == session).sum())
                rec[f"{session}_Touch_Bars"] = n_touches
                rec[f"{session}_Revisited"] = n_touches > 0

            if not touches.empty:
                first_close = float(day.loc[first_ts, "Close"])
                post = day.loc[day.index >= first_ts]
                rec.update({
                    "First_Revisit_Timestamp": first_ts,
                    "First_Revisit_Weekday": trading_weekday(first_ts),
                    "First_Revisit_Session": session_of(first_ts),
                    "First_Revisit_Hour": first_ts.hour,
                    "First_Revisit_Minute": first_ts.minute,
                    "Minutes_To_First_Revisit": (first_ts - level_ts).total_seconds() / 60,
                    "High_Already_Formed_At_Revisit": bool(day_high_ts <= first_ts),
                    "Low_Already_Formed_At_Revisit": bool(day_low_ts <= first_ts),
                    "Post_Revisit_High_Excursion": float(post["High"].max() - level_value),
                    "Post_Revisit_Low_Excursion": float(level_value - post["Low"].min()),
                    "Day_Close_Return_From_Revisit_Pct": (day_close / first_close - 1) * 100 if first_close else np.nan,
                    "Next_Session_Return_From_Revisit_Pct": _next_session_return(day, first_ts),
                })
            else:
                rec.update({
                    "First_Revisit_Timestamp": pd.NaT,
                    "First_Revisit_Weekday": np.nan,
                    "First_Revisit_Session": np.nan,
                    "First_Revisit_Hour": np.nan,
                    "First_Revisit_Minute": np.nan,
                    "Minutes_To_First_Revisit": np.nan,
                    "High_Already_Formed_At_Revisit": False,
                    "Low_Already_Formed_At_Revisit": False,
                    "Post_Revisit_High_Excursion": np.nan,
                    "Post_Revisit_Low_Excursion": np.nan,
                    "Day_Close_Return_From_Revisit_Pct": np.nan,
                    "Next_Session_Return_From_Revisit_Pct": np.nan,
                })
            records.append(rec)
    return pd.DataFrame.from_records(records)


def intraday_level_revisit_distribution(rows: pd.DataFrame) -> pd.DataFrame:
    """Revisit rate and touch count by split and level."""
    records = []
    for keys, group in rows.groupby(["Split", "Level_Name"], dropna=False):
        split, level = keys
        n = len(group)
        revisits = int(group["Revisited"].sum())
        rec = {
            "Split": split,
            "Level_Name": level,
            "n": n,
            "revisit_days": revisits,
            "revisit_pct": round(revisits / n * 100, 4) if n else np.nan,
            "avg_touch_bars": round(float(group["Touch_Bars_Total"].mean()), 4) if n else np.nan,
            "median_touch_bars": round(float(group["Touch_Bars_Total"].median()), 4) if n else np.nan,
        }
        for session in SESSION_ORDER:
            col = f"{session}_Revisited"
            if col in group:
                rec[f"{session}_revisit_pct"] = round(float(group[col].mean() * 100), 4)
        records.append(rec)
    return pd.DataFrame(records)


def intraday_level_first_revisit_distribution(rows: pd.DataFrame) -> pd.DataFrame:
    """First revisit distribution by split, level, and session."""
    clean = rows[rows["Revisited"]].dropna(subset=["First_Revisit_Session"])
    if clean.empty:
        return pd.DataFrame(columns=["Split", "Level_Name", "First_Revisit_Session", "n", "pct"])
    out = clean.groupby(["Split", "Level_Name", "First_Revisit_Session"], dropna=False).size().reset_index(name="n")
    totals = out.groupby(["Split", "Level_Name"])["n"].transform("sum")
    out["pct"] = (out["n"] / totals * 100).round(4)
    return out.sort_values(["Split", "Level_Name", "pct"], ascending=[True, True, False]).reset_index(drop=True)


def intraday_level_path_outcomes(rows: pd.DataFrame) -> pd.DataFrame:
    """Path/extreme outcomes by split, level, and first revisit session."""
    clean = rows[rows["Revisited"]].dropna(subset=["First_Revisit_Session"])
    records = []
    def avg_col(group: pd.DataFrame, col: str) -> float:
        if col not in group:
            return np.nan
        return round(float(group[col].mean()), 4)

    for keys, group in clean.groupby(["Split", "Level_Name", "First_Revisit_Session"], dropna=False):
        split, level, session = keys
        n = len(group)
        records.append({
            "Split": split,
            "Level_Name": level,
            "First_Revisit_Session": session,
            "n": n,
            "day_bullish_pct": round(float(group["Day_Bullish"].mean() * 100), 4),
            "close_above_level_pct": round(float(group["Day_Close_Above_Level"].mean() * 100), 4),
            "high_already_formed_pct": round(float(group["High_Already_Formed_At_Revisit"].mean() * 100), 4),
            "low_already_formed_pct": round(float(group["Low_Already_Formed_At_Revisit"].mean() * 100), 4),
            "avg_day_close_return_from_revisit_pct": avg_col(group, "Day_Close_Return_From_Revisit_Pct"),
            "avg_next_session_return_from_revisit_pct": avg_col(group, "Next_Session_Return_From_Revisit_Pct"),
            "avg_post_revisit_high_excursion": avg_col(group, "Post_Revisit_High_Excursion"),
            "avg_post_revisit_low_excursion": avg_col(group, "Post_Revisit_Low_Excursion"),
        })
    return pd.DataFrame(records)



def _bucket_label(ts: pd.Timestamp) -> str:
    return f"{ts.hour:02d}:{ts.minute:02d}"


def _bucket_session_minute(ts: pd.Timestamp) -> int:
    """Minutes since the 18:00 ET session open, so buckets sort chronologically."""
    return (ts.hour * 60 + ts.minute - 18 * 60) % (24 * 60)


def intraday_level_forward_touch_distribution(df_1m: pd.DataFrame, bucket_freq: str = "15min") -> pd.DataFrame:
    """
    For each trading day × level × later time bucket, estimate probability that
    the level is touched at or after the bucket start before same trading day end.
    """
    agg = {}
    day_keys = intraday_trading_day_index(df_1m.index)
    for trading_day, day in df_1m.groupby(day_keys, sort=True):
        if len(day) < 2:
            continue
        day = day.sort_index()
        split = "Train" if trading_day <= TRAIN_END else "OOS"
        hours = day.index.hour
        minutes = day.index.minute
        lows = day["Low"].to_numpy()
        highs = day["High"].to_numpy()
        opens = day["Open"].to_numpy()
        for level_name, (hour, minute) in KEY_LEVEL_TIMES.items():
            level_positions = np.flatnonzero((hours == hour) & (minutes == minute))
            if len(level_positions) == 0:
                continue
            level_pos = int(level_positions[0])
            if level_pos >= len(day) - 1:
                continue
            level_value = float(opens[level_pos])
            touch_arr = (lows <= level_value) & (highs >= level_value)
            future_touch = np.maximum.accumulate(touch_arr[::-1])[::-1]
            after_first = day.index[level_pos + 1]
            after_last = day.index[-1]
            bucket_starts = pd.date_range(
                start=after_first.ceil(bucket_freq),
                end=after_last.floor(bucket_freq),
                freq=bucket_freq,
                tz=day.index.tz,
            )
            if len(bucket_starts) == 0:
                continue
            positions = day.index.searchsorted(bucket_starts, side="left")
            valid = positions < len(day)
            for bucket_start, pos in zip(bucket_starts[valid], positions[valid]):
                key = (split, level_name, bucket_freq, _bucket_label(bucket_start), _bucket_session_minute(bucket_start))
                cur = agg.setdefault(key, [0, 0])
                cur[0] += 1
                cur[1] += int(bool(future_touch[pos]))

    columns = ["Split", "Level_Name", "Bucket_Freq", "Bucket_Time", "Bucket_Session_Minute", "n", "touch_days", "touch_pct"]
    records = []
    for (split, level_name, freq, bucket_time, session_minute), (n, touch_days) in agg.items():
        records.append({
            "Split": split,
            "Level_Name": level_name,
            "Bucket_Freq": freq,
            "Bucket_Time": bucket_time,
            "Bucket_Session_Minute": session_minute,
            "n": n,
            "touch_days": touch_days,
            "touch_pct": round(touch_days / n * 100, 4) if n else np.nan,
        })
    if not records:
        return pd.DataFrame(columns=columns)
    # Sort by minutes-since-18:00, not by the "HH:MM" string: a futures day starts
    # at 18:00, so lexicographic order draws the curve starting mid-session.
    return pd.DataFrame.from_records(records).sort_values(["Split", "Level_Name", "Bucket_Session_Minute"]).reset_index(drop=True)

def _forward_touch_chart(df: pd.DataFrame, freq_label: str, filename: str, symbol: str = None, output_dir: Path = None):
    train = df[df["Split"].eq("Train")].copy()
    if train.empty:
        return
    symbol = symbol or SYMBOL
    fig, axes = plt.subplots(len(KEY_LEVEL_TIMES), 1, figsize=(14, 12), sharex=True, sharey=True)
    if len(KEY_LEVEL_TIMES) == 1:
        axes = [axes]
    for ax, level_name in zip(axes, KEY_LEVEL_TIMES.keys()):
        sub = train[train["Level_Name"].eq(level_name)].sort_values("Bucket_Session_Minute")
        ax.plot(range(len(sub)), sub["touch_pct"], marker="o", markersize=2, linewidth=1.2)
        ax.set_title(level_name)
        ax.set_ylabel("Touch later (%)")
        ax.yaxis.set_major_formatter(mticker.PercentFormatter())
        ax.grid(alpha=0.25, linestyle="--")
        step = max(1, len(sub) // 12)
        ax.set_xticks(range(0, len(sub), step))
        ax.set_xticklabels(sub["Bucket_Time"].iloc[::step], rotation=45, ha="right")
    fig.suptitle(f"{symbol} probability level is touched after bucket start ({freq_label}, Train; x-axis runs 18:00 → 17:00)")
    fig.tight_layout()
    _save(fig, filename, output_dir=output_dir)

def run_intraday_key_level_research(path: str = DATA_PATH, symbol: str = None, output_dir: Path = None) -> pd.DataFrame:
    """Generate intraday key-level revisit/path research from source 1m data."""
    symbol = symbol or SYMBOL
    out_dir = output_dir or module_output_dir("intraday_levels", symbol)
    print(f"[Research] {symbol} intraday key-level revisits from 1m source ...")
    df_1m = load_1m_source(path)
    rows = build_intraday_key_level_rows(df_1m)
    _write_csv(rows, "intraday_level_rows", output_dir=out_dir)

    dist = intraday_level_revisit_distribution(rows)
    _write_csv(dist, "intraday_level_revisit_distribution", output_dir=out_dir)
    _barh_chart(dist[dist["Split"].eq("Train")].sort_values("revisit_pct", ascending=False), ["Level_Name"], "revisit_pct", f"{symbol} intraday key-level revisit rate (Train)", "intraday_level_revisit_distribution", output_dir=out_dir)

    first = intraday_level_first_revisit_distribution(rows)
    _write_csv(first, "intraday_level_first_revisit_by_session", output_dir=out_dir)
    _barh_chart(first[first["Split"].eq("Train")].sort_values("pct", ascending=False), ["Level_Name", "First_Revisit_Session"], "pct", f"{symbol} first revisit session by key level (Train)", "intraday_level_first_revisit_by_session", output_dir=out_dir)

    outcomes = intraday_level_path_outcomes(rows)
    _write_csv(outcomes, "intraday_level_path_outcomes", output_dir=out_dir)

    forward_15m = intraday_level_forward_touch_distribution(df_1m, bucket_freq="15min")
    _write_csv(forward_15m, "intraday_level_forward_touch_15m", output_dir=out_dir)
    _forward_touch_chart(forward_15m, "15m", "intraday_level_forward_touch_15m", symbol=symbol, output_dir=out_dir)

    forward_1h = intraday_level_forward_touch_distribution(df_1m, bucket_freq="1h")
    _write_csv(forward_1h, "intraday_level_forward_touch_1h", output_dir=out_dir)
    _forward_touch_chart(forward_1h, "1h", "intraday_level_forward_touch_1h", symbol=symbol, output_dir=out_dir)

    print("\n[Research] Intraday key-level revisit distribution:")
    print(dist.to_string(index=False))
    print("\n[Research] Intraday key-level path outcomes:")
    print(outcomes.head(30).to_string(index=False))
    return rows



# ═══════════════════════════════════════════════════════════════════════════════
# INTRADAY → WEEKLY PATH DEPENDENCY
# ═══════════════════════════════════════════════════════════════════════════════

PATH_DEPENDENCY_METRICS = [
    "Minutes_To_First_Revisit",
    "Touch_Bars_Total",
    "Post_Revisit_High_Excursion",
    "Post_Revisit_Low_Excursion",
]


def apply_path_dependency_buckets(rows: pd.DataFrame) -> pd.DataFrame:
    """Apply train p25/p75 buckets per level, keeping no-revisit as own bucket."""
    out = rows.copy()
    for metric in PATH_DEPENDENCY_METRICS:
        bucket_col = f"{metric}_Bucket"
        out[bucket_col] = "Middle 25-75%"
        out.loc[~out["Revisited"].astype(bool), bucket_col] = "No Revisit"
        for level_name, level_rows in out.groupby("Level_Name", dropna=False):
            train = level_rows[(level_rows["Split"].eq("Train")) & (level_rows["Revisited"].astype(bool))]
            vals = train[metric].dropna() if metric in train else pd.Series(dtype=float)
            if vals.empty:
                continue
            q25, q75 = vals.quantile([0.25, 0.75])
            mask = out["Level_Name"].eq(level_name) & out["Revisited"].astype(bool) & out[metric].notna()
            out.loc[mask & (out[metric] <= q25), bucket_col] = "P25 Low"
            out.loc[mask & (out[metric] >= q75), bucket_col] = "P75 High"
    return out


def _pct(series: pd.Series) -> float:
    return round(float(series.mean() * 100), 4) if len(series) else np.nan


def intraday_to_weekly_path_dependency(
    intraday_rows: pd.DataFrame,
    weekly: pd.DataFrame,
    event_rows: pd.DataFrame = None,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Join intraday key-level path buckets to weekly outcomes and summarize."""
    detail = apply_path_dependency_buckets(intraday_rows)
    detail["Week_Start"] = pd.to_datetime(detail["Trading_Day"]).map(trading_week_monday)

    weekly_cols = ["Bull_Bear", "High_Weekday", "Low_Weekday", "High_Session", "Low_Session"]
    weekly_join = weekly[weekly_cols].copy()
    weekly_join.index.name = "Week_Start"
    detail = detail.merge(weekly_join.reset_index(), on="Week_Start", how="left")
    detail["Week_Bullish"] = detail["Bull_Bear"].eq("Bullish")
    detail["Weekly_High_Friday"] = detail["High_Weekday"].eq("Friday")
    detail["Weekly_Low_Monday"] = detail["Low_Weekday"].eq("Monday")
    detail["Weekly_Low_Friday"] = detail["Low_Weekday"].eq("Friday")
    detail["Weekly_High_Monday"] = detail["High_Weekday"].eq("Monday")

    if event_rows is not None and not event_rows.empty:
        events = (
            event_rows.groupby("Week_Start", as_index=False)
            .agg(Final_High_Timestamp=("Final_High_Timestamp", "first"), Final_Low_Timestamp=("Final_Low_Timestamp", "first"))
        )
        detail = detail.merge(events, on="Week_Start", how="left")
        day_end = pd.to_datetime(detail["Trading_Day"]) + pd.Timedelta(hours=17)
        detail["Weekly_High_Formed_By_Day"] = pd.to_datetime(detail["Final_High_Timestamp"]) <= day_end
        detail["Weekly_Low_Formed_By_Day"] = pd.to_datetime(detail["Final_Low_Timestamp"]) <= day_end
    else:
        detail["Weekly_High_Formed_By_Day"] = np.nan
        detail["Weekly_Low_Formed_By_Day"] = np.nan

    detail["Weekday"] = trading_weekday_index(pd.DatetimeIndex(detail["Trading_Day"]))

    records = []
    for metric in PATH_DEPENDENCY_METRICS:
        bucket_col = f"{metric}_Bucket"
        # Weekday is part of the key: a weekly label is contemporaneous with a
        # Friday row and forward-looking for a Monday row. Pooling them mixes
        # prediction with description and repeats one weekly label up to 5 times.
        for keys, group in detail.groupby(["Split", "Weekday", "Level_Name", bucket_col], dropna=False):
            split, weekday, level_name, bucket = keys
            records.append({
                "Split": split,
                "Weekday": weekday,
                "Level_Name": level_name,
                "Metric": metric,
                "Metric_Bucket": bucket,
                "n": len(group),
                "n_weeks": int(group["Week_Start"].nunique()),
                "day_bullish_pct": _pct(group["Day_Bullish"]),
                "day_close_above_level_pct": _pct(group["Day_Close_Above_Level"]),
                "week_bullish_pct": _pct(group["Week_Bullish"]),
                "weekly_high_friday_pct": _pct(group["Weekly_High_Friday"]),
                "weekly_low_monday_pct": _pct(group["Weekly_Low_Monday"]),
                "weekly_low_friday_pct": _pct(group["Weekly_Low_Friday"]),
                "weekly_high_monday_pct": _pct(group["Weekly_High_Monday"]),
                "weekly_high_formed_by_day_pct": _pct(group["Weekly_High_Formed_By_Day"].dropna()) if group["Weekly_High_Formed_By_Day"].notna().any() else np.nan,
                "weekly_low_formed_by_day_pct": _pct(group["Weekly_Low_Formed_By_Day"].dropna()) if group["Weekly_Low_Formed_By_Day"].notna().any() else np.nan,
            })
    summary = pd.DataFrame.from_records(records).sort_values(["Split", "Weekday", "Level_Name", "Metric", "Metric_Bucket"]).reset_index(drop=True)
    return detail, summary


def run_intraday_to_weekly_path_dependency_research(symbol: str = None, path: str = DATA_PATH, output_dir: Path = None, resample_to: str = None) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Run intraday → weekly path dependency research for one symbol."""
    symbol = symbol or SYMBOL
    out_dir = output_dir or module_output_dir("path_dependency", symbol)
    print(f"[Research] {symbol} intraday → weekly path dependency ...")
    df_1m = load_1m_source(path)
    df = load_and_resample(path, resample_to or RESAMPLE_TO)
    weekly = build_weekly(df)
    event_rows = build_event_rows(df)
    intraday = build_intraday_key_level_rows(df_1m)
    detail, summary = intraday_to_weekly_path_dependency(intraday, weekly, event_rows)
    _write_csv(detail, "intraday_to_weekly_path_dependency_detail", output_dir=out_dir)
    _write_csv(summary, "intraday_to_weekly_path_dependency_summary", output_dir=out_dir)
    return detail, summary



# ═══════════════════════════════════════════════════════════════════════════════
# RELATIVE LEVEL PATH RESEARCH
# ═══════════════════════════════════════════════════════════════════════════════

RELATIVE_WINDOWS = {
    "Globex_to_Midnight": ((18, 0), (0, 0)),
    "Midnight_to_0930": ((0, 0), (9, 30)),
    "0930_to_1300": ((9, 30), (13, 0)),
    "1300_to_Close": ((13, 0), (17, 0)),
}
RELATIVE_PATH_FEATURES = [
    "Pct_Bars_Above_Level",
    "Pct_Bars_Below_Level",
    "Pct_Bars_Touching_Level",
    "Mean_Distance_Above",
    "Mean_Distance_Below",
    "Max_Distance_Above",
    "Max_Distance_Below",
    "TWAP_Distance_To_Level",
    "VWAP_Distance_To_Level",
    "Composite_Min_Distance_To_Level",
    "Composite_Max_Distance_To_Level",
    "Composite_N_Levels_Above_Close",
    "Composite_N_Levels_Below_Close",
]
CATEGORICAL_RELATIVE_PATH_FEATURES = ["Composite_Level_State"]


def composite_level_context(window_close: float, level_values: dict) -> dict:
    """Classify close relative to all available key levels."""
    vals = [float(v) for v in level_values.values() if not pd.isna(v)]
    if not vals:
        return {
            "Composite_Level_State": "No Levels",
            "Composite_Levels_Defined": 0,
            "Composite_Min_Distance_To_Level": np.nan,
            "Composite_Max_Distance_To_Level": np.nan,
            "Composite_N_Levels_Above_Close": 0,
            "Composite_N_Levels_Below_Close": 0,
        }
    distances = [window_close - v for v in vals]
    n_above_close = sum(v > window_close for v in vals)
    n_below_close = sum(v < window_close for v in vals)
    if n_above_close == 0:
        state = "Above_All"
    elif n_below_close == 0:
        state = "Below_All"
    else:
        state = "Between"
    return {
        "Composite_Level_State": state,
        "Composite_Levels_Defined": len(vals),
        "Composite_Min_Distance_To_Level": round(float(min(abs(d) for d in distances)), 4),
        "Composite_Max_Distance_To_Level": round(float(max(abs(d) for d in distances)), 4),
        "Composite_N_Levels_Above_Close": int(n_above_close),
        "Composite_N_Levels_Below_Close": int(n_below_close),
    }


def _window_timestamp(trading_day: pd.Timestamp, hm: tuple[int, int]) -> pd.Timestamp:
    hour, minute = hm
    base = _shift_days_local(trading_day, -1) if hour >= 18 else trading_day
    return base.replace(hour=hour, minute=minute)


def build_relative_level_path_rows(df_1m: pd.DataFrame) -> pd.DataFrame:
    """Build time/distance features by trading day × key level × pre-session window."""
    records = []
    day_keys = intraday_trading_day_index(df_1m.index)
    for trading_day, day in df_1m.groupby(day_keys, sort=True):
        if len(day) < 2:
            continue
        day = day.sort_index()
        idx = day.index
        hours = idx.hour
        minutes = idx.minute
        opens = day["Open"].to_numpy()
        highs = day["High"].to_numpy()
        lows = day["Low"].to_numpy()
        closes = day["Close"].to_numpy()
        volumes = day["Volume"].to_numpy()
        sessions = session_of_index(idx)
        day_open = float(opens[0])
        day_close = float(closes[-1])

        level_values = {}
        level_timestamps = {}
        for level_name, (hour, minute) in KEY_LEVEL_TIMES.items():
            positions = np.flatnonzero((hours == hour) & (minutes == minute))
            if len(positions):
                pos = int(positions[0])
                level_values[level_name] = float(opens[pos])
                level_timestamps[level_name] = idx[pos]

        for level_name, level_value in level_values.items():
            for window_name, (start_hm, end_hm) in RELATIVE_WINDOWS.items():
                start_ts = _window_timestamp(trading_day, start_hm)
                end_ts = _window_timestamp(trading_day, end_hm)
                if end_ts <= start_ts:
                    end_ts += pd.Timedelta(days=1)
                start_pos = int(idx.searchsorted(start_ts, side="left"))
                end_pos = int(idx.searchsorted(end_ts, side="left"))
                if end_pos <= start_pos:
                    continue
                w_open = opens[start_pos]
                w_close = closes[end_pos - 1]
                w_highs = highs[start_pos:end_pos]
                w_lows = lows[start_pos:end_pos]
                w_closes = closes[start_pos:end_pos]
                w_volumes = volumes[start_pos:end_pos]
                above = w_closes > level_value
                below = w_closes < level_value
                touching = (w_lows <= level_value) & (w_highs >= level_value)
                dist_above = np.maximum(w_highs - level_value, 0)
                dist_below = np.maximum(level_value - w_lows, 0)
                window_twap = float(np.mean(w_closes))
                typical = (w_highs + w_lows + w_closes) / 3
                vol_sum = float(np.sum(w_volumes))
                # No volume means no VWAP. Falling back to TWAP would silently make
                # the two signals identical instead of flagging the window as unusable.
                window_vwap = float(np.sum(typical * w_volumes) / vol_sum) if vol_sum > 0 else np.nan
                prior_levels = {name: val for name, val in level_values.items() if level_timestamps[name] <= end_ts}
                composite = composite_level_context(float(w_close), prior_levels)

                next_return = np.nan
                next_bullish = np.nan
                bounds = _next_session_bounds(sessions, end_pos - 1)
                if bounds is not None:
                    next_close = float(closes[bounds[1]])
                    start_close = float(w_close)
                    next_return = (next_close / start_close - 1) * 100 if start_close else np.nan
                    next_bullish = bool(next_close > start_close)

                records.append({
                    "Trading_Day": trading_day,
                    "Split": "Train" if trading_day <= TRAIN_END else "OOS",
                    "Weekday": trading_weekday(trading_day),
                    "Level_Name": level_name,
                    "Level_Value": level_value,
                    "Window_Name": window_name,
                    "Window_Start": start_ts,
                    "Window_End": end_ts,
                    # False when the level is only defined AFTER the window closes
                    # (e.g. the 13:00 open measured over Globex→Midnight). Those
                    # combinations are look-ahead and are excluded from summaries.
                    "Level_Defined_By_Window_End": bool(level_timestamps[level_name] <= end_ts),
                    "Bars_Total": int(len(w_closes)),
                    "Pct_Bars_Above_Level": round(float(above.mean() * 100), 4),
                    "Pct_Bars_Below_Level": round(float(below.mean() * 100), 4),
                    "Pct_Bars_Touching_Level": round(float(touching.mean() * 100), 4),
                    "Mean_Distance_Above": round(float(dist_above.mean()), 4),
                    "Mean_Distance_Below": round(float(dist_below.mean()), 4),
                    "Max_Distance_Above": round(float(dist_above.max()), 4),
                    "Max_Distance_Below": round(float(dist_below.max()), 4),
                    "Window_TWAP": round(window_twap, 4),
                    "Window_VWAP": round(window_vwap, 4) if not pd.isna(window_vwap) else np.nan,
                    "TWAP_Distance_To_Level": round(window_twap - level_value, 4),
                    "VWAP_Distance_To_Level": round(window_vwap - level_value, 4) if not pd.isna(window_vwap) else np.nan,
                    "TWAP_Above_Level": bool(window_twap > level_value),
                    "VWAP_Above_Level": bool(window_vwap > level_value) if not pd.isna(window_vwap) else None,
                    **composite,
                    "Window_Close_Above_Level": bool(w_close > level_value),
                    "Window_Return_Pct": (w_close / w_open - 1) * 100 if w_open else np.nan,
                    "Next_Session_Return_Pct": next_return,
                    "Next_Session_Bullish": next_bullish,
                    "Day_Bullish": bool(day_close > day_open),
                    "Day_Close_Above_Level": bool(day_close > level_value),
                    "Day_Return_Pct": (day_close / day_open - 1) * 100 if day_open else np.nan,
                })
    return pd.DataFrame.from_records(records)

def apply_relative_path_buckets(rows: pd.DataFrame, features: list = None) -> pd.DataFrame:
    """Apply train p25/p75 buckets per level × window × feature."""
    features = features or RELATIVE_PATH_FEATURES
    out = rows.copy()
    for feature in features:
        bucket_col = f"{feature}_Bucket"
        out[bucket_col] = "Middle 25-75%"
        for keys, group in out.groupby(["Level_Name", "Window_Name"], dropna=False):
            level_name, window_name = keys
            vals = group[group["Split"].eq("Train")][feature].dropna()
            if vals.empty:
                continue
            q25, q75 = vals.quantile([0.25, 0.75])
            mask = out["Level_Name"].eq(level_name) & out["Window_Name"].eq(window_name) & out[feature].notna()
            out.loc[mask & (out[feature] <= q25), bucket_col] = "P25 Low"
            out.loc[mask & (out[feature] >= q75), bucket_col] = "P75 High"
    return out


def drop_lookahead_level_windows(rows: pd.DataFrame, include_lookahead: bool = False) -> pd.DataFrame:
    """
    Keep only level × window pairs where the level exists before the window ends.

    `build_relative_level_path_rows` crosses every key level with every window, so
    3 of the 16 combinations per day measure bars against a level that is not
    defined until hours later (e.g. the 13:00 open over Globex→Midnight). Those
    rows are look-ahead and must not reach a conditional summary.
    """
    if include_lookahead or "Level_Defined_By_Window_End" not in rows:
        return rows
    return rows[rows["Level_Defined_By_Window_End"].astype(bool)]


def relative_path_intraday_outcomes(rows: pd.DataFrame, features: list = None, include_lookahead: bool = False) -> pd.DataFrame:
    """Summarize next-session and daily outcomes by relative path buckets."""
    features = features or RELATIVE_PATH_FEATURES
    detail = apply_relative_path_buckets(drop_lookahead_level_windows(rows, include_lookahead), features)
    records = []
    for feature in features:
        bucket_col = f"{feature}_Bucket"
        for keys, group in detail.groupby(["Split", "Level_Name", "Window_Name", bucket_col], dropna=False):
            split, level, window, bucket = keys
            records.append({
                "Split": split,
                "Level_Name": level,
                "Window_Name": window,
                "Feature": feature,
                "Feature_Bucket": bucket,
                "n": len(group),
                "next_session_bullish_pct": _pct(group["Next_Session_Bullish"].dropna()) if group["Next_Session_Bullish"].notna().any() else np.nan,
                "avg_next_session_return_pct": round(float(group["Next_Session_Return_Pct"].mean()), 4),
                "day_bullish_pct": _pct(group["Day_Bullish"]),
                "day_close_above_level_pct": _pct(group["Day_Close_Above_Level"]),
                "avg_day_return_pct": round(float(group["Day_Return_Pct"].mean()), 4),
            })
    for cat_feature in CATEGORICAL_RELATIVE_PATH_FEATURES:
        if cat_feature not in detail:
            continue
        for keys, group in detail.groupby(["Split", "Level_Name", "Window_Name", cat_feature], dropna=False):
            split, level, window, bucket = keys
            records.append({
                "Split": split,
                "Level_Name": level,
                "Window_Name": window,
                "Feature": cat_feature,
                "Feature_Bucket": bucket,
                "n": len(group),
                "next_session_bullish_pct": _pct(group["Next_Session_Bullish"].dropna()) if group["Next_Session_Bullish"].notna().any() else np.nan,
                "avg_next_session_return_pct": round(float(group["Next_Session_Return_Pct"].mean()), 4),
                "day_bullish_pct": _pct(group["Day_Bullish"]),
                "day_close_above_level_pct": _pct(group["Day_Close_Above_Level"]),
                "avg_day_return_pct": round(float(group["Day_Return_Pct"].mean()), 4),
            })
    return pd.DataFrame.from_records(records).sort_values(["Split", "Level_Name", "Window_Name", "Feature", "Feature_Bucket"]).reset_index(drop=True)


def relative_path_to_weekly_outcomes(rows: pd.DataFrame, weekly: pd.DataFrame, event_rows: pd.DataFrame = None, features: list = None, include_lookahead: bool = False) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Summarize Monday/Tuesday relative path buckets vs weekly outcomes."""
    features = features or RELATIVE_PATH_FEATURES
    detail = apply_relative_path_buckets(drop_lookahead_level_windows(rows, include_lookahead), features)
    detail = detail[detail["Weekday"].isin(["Monday", "Tuesday"])].copy()
    detail["Week_Start"] = pd.to_datetime(detail["Trading_Day"]).map(trading_week_monday)
    weekly_cols = ["Bull_Bear", "High_Weekday", "Low_Weekday", "High_Session", "Low_Session"]
    weekly_join = weekly[weekly_cols].copy()
    weekly_join.index.name = "Week_Start"
    detail = detail.merge(weekly_join.reset_index(), on="Week_Start", how="left")
    detail["Week_Bullish"] = detail["Bull_Bear"].eq("Bullish")
    detail["Weekly_High_Friday"] = detail["High_Weekday"].eq("Friday")
    detail["Weekly_Low_Monday"] = detail["Low_Weekday"].eq("Monday")
    detail["Weekly_Low_Friday"] = detail["Low_Weekday"].eq("Friday")
    detail["Weekly_High_Monday"] = detail["High_Weekday"].eq("Monday")
    if event_rows is not None and not event_rows.empty:
        events = event_rows.groupby("Week_Start", as_index=False).agg(Final_High_Timestamp=("Final_High_Timestamp", "first"), Final_Low_Timestamp=("Final_Low_Timestamp", "first"))
        detail = detail.merge(events, on="Week_Start", how="left")
        day_end = pd.to_datetime(detail["Trading_Day"]) + pd.Timedelta(hours=17)
        detail["Weekly_High_Formed_By_Day"] = pd.to_datetime(detail["Final_High_Timestamp"]) <= day_end
        detail["Weekly_Low_Formed_By_Day"] = pd.to_datetime(detail["Final_Low_Timestamp"]) <= day_end
    else:
        detail["Weekly_High_Formed_By_Day"] = np.nan
        detail["Weekly_Low_Formed_By_Day"] = np.nan
    records = []
    for feature in features:
        bucket_col = f"{feature}_Bucket"
        for keys, group in detail.groupby(["Split", "Weekday", "Level_Name", "Window_Name", bucket_col], dropna=False):
            split, weekday, level, window, bucket = keys
            records.append({
                "Split": split,
                "Weekday": weekday,
                "Level_Name": level,
                "Window_Name": window,
                "Feature": feature,
                "Feature_Bucket": bucket,
                "n": len(group),
                "n_weeks": int(group["Week_Start"].nunique()),
                "week_bullish_pct": _pct(group["Week_Bullish"]),
                "weekly_high_friday_pct": _pct(group["Weekly_High_Friday"]),
                "weekly_low_monday_pct": _pct(group["Weekly_Low_Monday"]),
                "weekly_low_friday_pct": _pct(group["Weekly_Low_Friday"]),
                "weekly_high_monday_pct": _pct(group["Weekly_High_Monday"]),
                "weekly_high_formed_by_day_pct": _pct(group["Weekly_High_Formed_By_Day"].dropna()) if group["Weekly_High_Formed_By_Day"].notna().any() else np.nan,
                "weekly_low_formed_by_day_pct": _pct(group["Weekly_Low_Formed_By_Day"].dropna()) if group["Weekly_Low_Formed_By_Day"].notna().any() else np.nan,
            })
    for cat_feature in CATEGORICAL_RELATIVE_PATH_FEATURES:
        if cat_feature not in detail:
            continue
        for keys, group in detail.groupby(["Split", "Weekday", "Level_Name", "Window_Name", cat_feature], dropna=False):
            split, weekday, level, window, bucket = keys
            records.append({
                "Split": split,
                "Weekday": weekday,
                "Level_Name": level,
                "Window_Name": window,
                "Feature": cat_feature,
                "Feature_Bucket": bucket,
                "n": len(group),
                "n_weeks": int(group["Week_Start"].nunique()),
                "week_bullish_pct": _pct(group["Week_Bullish"]),
                "weekly_high_friday_pct": _pct(group["Weekly_High_Friday"]),
                "weekly_low_monday_pct": _pct(group["Weekly_Low_Monday"]),
                "weekly_low_friday_pct": _pct(group["Weekly_Low_Friday"]),
                "weekly_high_monday_pct": _pct(group["Weekly_High_Monday"]),
                "weekly_high_formed_by_day_pct": _pct(group["Weekly_High_Formed_By_Day"].dropna()) if group["Weekly_High_Formed_By_Day"].notna().any() else np.nan,
                "weekly_low_formed_by_day_pct": _pct(group["Weekly_Low_Formed_By_Day"].dropna()) if group["Weekly_Low_Formed_By_Day"].notna().any() else np.nan,
            })
    summary = pd.DataFrame.from_records(records).sort_values(["Split", "Weekday", "Level_Name", "Window_Name", "Feature", "Feature_Bucket"]).reset_index(drop=True)
    return detail, summary


def run_relative_path_research(symbol: str = None, path: str = DATA_PATH, output_dir: Path = None, resample_to: str = None) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """Run relative level path research for one symbol."""
    symbol = symbol or SYMBOL
    out_dir = output_dir or module_output_dir("relative_path", symbol)
    print(f"[Research] {symbol} relative level path ...")
    df_1m = load_1m_source(path)
    df = (
        df_1m.resample(resample_to or RESAMPLE_TO, label="left", closed="left")
        .agg({"Open": "first", "High": "max", "Low": "min", "Close": "last", "Volume": "sum"})
        .dropna(subset=["Open"])
    )
    weekly = build_weekly(df)
    event_rows = build_event_rows(df)
    rows = build_relative_level_path_rows(df_1m)
    intraday = relative_path_intraday_outcomes(rows)
    detail, weekly_summary = relative_path_to_weekly_outcomes(rows, weekly, event_rows)
    _write_csv(rows, "relative_path_detail", output_dir=out_dir)
    _write_csv(intraday, "relative_path_intraday_outcomes", output_dir=out_dir)
    _write_csv(detail, "relative_path_early_week_detail", output_dir=out_dir)
    _write_csv(weekly_summary, "relative_path_early_week_weekly_outcomes", output_dir=out_dir)
    return rows, intraday, weekly_summary


# ═══════════════════════════════════════════════════════════════════════════════
# MONTHLY AGGREGATION & EXTREME TIMING
# ═══════════════════════════════════════════════════════════════════════════════
#
# Monthly outputs carry NO train/OOS split. The ES sample holds 194 months against
# 841 weeks; a 2023-12-31 split leaves 31 OOS months, so a five-way conditional cut
# gives ~6 observations per cell. Reporting that as out-of-sample validation would
# manufacture confidence the sample cannot support. Every figure instead carries n,
# a bootstrap 95% CI, and is_sparse against SPARSE_MONTHS. No "strongest cell"
# ranking is produced: with ~16 observations per month-of-year, a top-N of a wide
# grid is noise mining.


def build_monthly(df: pd.DataFrame) -> pd.DataFrame:
    """
    One row per session month, with extreme timing expressed as session position.

    A session month holds every bar whose session date falls in that calendar
    month, so it opens on the 18:00 ET print of the last session of the previous
    month. Position features are 0-based indexes into the month's ordered session
    list, normalized by length, so months holding 13–24 sessions stay comparable.

    `N_Sessions` comes from the exchange calendar, not from price. It is published
    in advance, so normalized position does not violate the knowability rule that
    governs the month-state conditioners.
    """
    if df.empty:
        return pd.DataFrame()
    month_keys = trading_month_start_index(df.index)
    sessions = pd.Series(intraday_trading_day_index(df.index), index=df.index)
    first_month, last_month = month_keys.min(), month_keys.max()

    records = []
    for month_start, m in df.groupby(month_keys, sort=True):
        if len(m) < 2:
            continue
        m = m.sort_index()
        month_sessions = sessions.loc[m.index]
        ordered = pd.DatetimeIndex(pd.unique(month_sessions))
        n_sessions = len(ordered)
        position = {session: i for i, session in enumerate(ordered)}
        month_open = float(m["Open"].iloc[0])
        month_close = float(m["Close"].iloc[-1])
        rec = {
            "Month_Start": month_start,
            "Month_Of_Year": int(month_start.month),
            "N_Sessions": n_sessions,
            # The sample starts and ends mid-month; both edge months would otherwise
            # contribute a truncated high/low. They are excluded from every rate.
            "Is_Partial": bool(month_start == first_month or month_start == last_month),
            "Bull_Bear": "Bullish" if month_close > month_open else "Bearish",
            "Monthly_Open": month_open,
            "Monthly_Close": month_close,
            "Monthly_High": float(m["High"].max()),
            "Monthly_Low": float(m["Low"].min()),
        }
        for label, ts in (("High", m["High"].idxmax()), ("Low", m["Low"].idxmin())):
            session = month_sessions.loc[ts]
            index = position[session]
            rec.update({
                f"{label}_Session_Index": index,
                f"{label}_Session_Pos": round(index / (n_sessions - 1), 6) if n_sessions > 1 else 0.0,
                f"{label}_Third": _position_bucket(index, n_sessions, MONTH_THIRDS),
                f"{label}_Quintile": _position_bucket(index, n_sessions, MONTH_QUINTILES),
                f"{label}_Week_Of_Month": week_of_month(session),
                f"{label}_Weekday": trading_weekday(ts),
                f"{label}_Session": session_of(ts),
                f"{label}_Hour": int(ts.hour),
                f"{label}_Day_Of_Month": int(session.day),
            })
        records.append(rec)

    out = pd.DataFrame.from_records(records)
    if not out.empty:
        out["Prev_Bull_Bear"] = out["Bull_Bear"].shift(1)
    return out


def complete_months(monthly: pd.DataFrame) -> pd.DataFrame:
    """Drop the truncated first and last months; they cannot hold a real high/low."""
    if monthly.empty:
        return monthly
    return monthly[~monthly["Is_Partial"].astype(bool)].copy()


MONTH_TIMING_CUTS = {
    "third": ("Third", MONTH_THIRDS),
    "quintile": ("Quintile", MONTH_QUINTILES),
    "week_of_month": ("Week_Of_Month", WEEK_OF_MONTH_ORDER),
    "weekday": ("Weekday", DAYS),
    "session": ("Session", SESSION_ORDER),
    "day_of_month": ("Day_Of_Month", list(range(1, 32))),
}
MONTH_PRIMARY_CUTS = ["third", "quintile", "week_of_month"]
# Cuts that measure POSITION in the month, so the arcsine law applies to them. The
# calendar cuts (weekday, session, week-of-month, day-of-month) are not equal-width
# slices of the period and have no arcsine baseline.
MONTH_POSITION_CUTS = ["third", "quintile"]


def monthly_share_table(monthly: pd.DataFrame, column: str, order: list = None,
                        sparse_months: int = SPARSE_MONTHS) -> pd.DataFrame:
    """
    Share of complete months whose `column` falls in each bucket, with a CI.

    `build_monthly` emits exactly one row per month, so the month IS the
    independent unit and `bootstrap_probability_ci` needs no `clusters` argument.
    Clustering is only needed where one label is spread across many correlated
    rows, as it is for the bar-level weekly work.

    `months` is the denominator, `n` the bucket count. Shares sum to 100 within a
    direction scope, so a bucket that only exists in some months needs the exposure
    columns from `month_week_exposure` to be readable.
    """
    rows = complete_months(monthly)
    scopes = [
        ("All", rows),
        ("Bullish", rows[rows["Bull_Bear"].eq("Bullish")]),
        ("Bearish", rows[rows["Bull_Bear"].eq("Bearish")]),
    ]
    records = []
    for scope, scope_rows in scopes:
        values = scope_rows[column]
        buckets = order if order is not None else sorted(values.dropna().unique().tolist(), key=str)
        for bucket in buckets:
            hits = values.eq(bucket)
            stats = bootstrap_probability_ci(hits)
            records.append({
                "Direction_Scope": scope,
                column: bucket,
                "months": int(len(scope_rows)),
                "n": int(hits.sum()),
                "pct": stats["probability"],
                "ci_low": stats["ci_low"],
                "ci_high": stats["ci_high"],
                "is_sparse": bool(int(hits.sum()) < sparse_months),
            })
    return pd.DataFrame.from_records(records)


def month_week_exposure(df: pd.DataFrame, monthly: pd.DataFrame) -> pd.DataFrame:
    """
    Sessions per calendar week bucket per complete month.

    A share means nothing without knowing how much of the period the bucket spans,
    and how often it exists at all. W5 appears in only ~41 of 192 ES months and
    covers ~2 sessions against W1–W4's ~5 — above the SPARSE_MONTHS floor, so the
    flag alone would not catch it. This is the generalization of the exposure fix
    applied to the weekly weekday buckets.
    """
    session_dates = pd.DatetimeIndex(pd.unique(intraday_trading_day_index(df.index)))
    frame = pd.DataFrame({
        "Month_Start": trading_month_start_index(session_dates),
        "Week_Of_Month": [week_of_month(d) for d in session_dates],
    })
    keep = set(complete_months(monthly)["Month_Start"])
    frame = frame[frame["Month_Start"].isin(keep)]
    counts = frame.groupby(["Month_Start", "Week_Of_Month"]).size().reset_index(name="sessions")
    out = (
        counts.groupby("Week_Of_Month")
        .agg(months_present=("Month_Start", "nunique"), avg_sessions=("sessions", "mean"))
        .reset_index()
    )
    out["avg_sessions"] = out["avg_sessions"].round(4)
    return out


def monthly_extreme_timing_tables(df: pd.DataFrame, monthly: pd.DataFrame) -> dict:
    """
    Every monthly extreme-timing cut, keyed by output filename stem.

    Primary cuts are session position (thirds, quintiles). Week-of-month, weekday,
    session and day-of-month are secondary views and the week-of-month table carries
    the exposure columns that make its non-comparable buckets visible.

    The position cuts also carry `arcsine_null_pct`: the share a driftless random walk
    puts in each bucket. It is the no-information baseline for these tables and it is
    NOT uniform — see `arcsine_null_shares`. Reading a 54% last-third share against
    33.3% overstates the effect by the 5.8pp the arcsine law supplies for free.
    """
    exposure = month_week_exposure(df, monthly)
    tables = {}
    for event in ["High", "Low"]:
        for cut, (suffix, order) in MONTH_TIMING_CUTS.items():
            column = f"{event}_{suffix}"
            table = monthly_share_table(monthly, column, order=order)
            if cut == "week_of_month":
                table = table.merge(
                    exposure.rename(columns={"Week_Of_Month": column}), on=column, how="left")
            if cut in MONTH_POSITION_CUTS and not table.empty:
                shares = dict(zip(order, arcsine_null_shares(len(order))))
                table["arcsine_null_pct"] = table[column].map(shares)
            tables[f"{event.lower()}_timing_by_{cut}"] = table
    return tables


def run_monthly_extremes_research(df: pd.DataFrame, symbol: str = None,
                                  output_dir: Path = None,
                                  n_sim: int = RW_NULL_SIMS) -> pd.DataFrame:
    """
    Monthly extreme-timing tables and charts from the resampled frame.

    Descriptive only — no train/OOS split, and no ranking table. Charts cover the
    primary position cuts plus week-of-month.

    The position cuts are also scored against the random-walk null ladder, because
    the share a month puts in its last third is mostly arithmetic: see the
    RANDOM-WALK NULL section.
    """
    symbol = symbol or SYMBOL
    out_dir = output_dir or module_output_dir("monthly_extremes", symbol)
    print(f"[Research] {symbol} monthly extremes ...")
    monthly = build_monthly(df)
    _write_csv(monthly, "monthly_rows", output_dir=out_dir)

    tables = monthly_extreme_timing_tables(df, monthly)
    for name, table in tables.items():
        _write_csv(table, name, output_dir=out_dir)

    print(f"[Research] {symbol} extreme timing vs the random-walk null ...")
    period_index = trading_month_start_index(df.index)
    position_index = intraday_trading_day_index(df.index)
    nulls = {}
    for cut in MONTH_POSITION_CUTS:
        _suffix, order = MONTH_TIMING_CUTS[cut]
        nulls[cut] = extreme_position_null(
            df, period_index, position_index, n_buckets=len(order), labels=order, n_sim=n_sim)
        _write_csv(nulls[cut], f"extreme_timing_null_by_{cut}", output_dir=out_dir)

    months = int(len(complete_months(monthly)))
    for event in ["high", "low"]:
        for cut in MONTH_PRIMARY_CUTS:
            suffix, _order = MONTH_TIMING_CUTS[cut]
            table = tables[f"{event}_timing_by_{cut}"]
            chart = table[table["Direction_Scope"].eq("All")]
            _barh_chart(
                chart,
                [f"{event.capitalize()}_{suffix}"],
                "pct",
                f"{symbol} monthly {event} by {cut.replace('_', ' ')} "
                f"— descriptive, {months} complete months",
                f"{event}_timing_by_{cut}",
                output_dir=out_dir,
            )

    print(f"\n[Research] Monthly high timing by third ({months} complete months):")
    print(tables["high_timing_by_third"].to_string(index=False))
    print("\n[Research] High timing vs the random-walk null ladder:")
    ladder = nulls["third"]
    print(ladder[ladder["Event"].eq("High")].to_string(index=False))
    return monthly


# ═══════════════════════════════════════════════════════════════════════════════
# RANDOM-WALK NULL
# ═══════════════════════════════════════════════════════════════════════════════
#
# "The monthly high formed in the last third 54% of the time" invites comparison
# against a uniform 33.3%. That baseline is wrong before any market behaviour is
# involved: for a driftless random walk the time of the maximum follows the ARCSINE
# LAW, F(t) = 2/pi * arcsin(sqrt t), which is U-shaped — 39.2% at each end and 21.6%
# in the middle. Add the sample's real upward drift and the last-third share rises
# again with no path structure whatsoever.
#
# This section supplies the ladder of nulls, each rung adding one real feature:
#
#   arcsine    closed form, randomness alone. No simulation, no data.
#   driftless  simulated zero-drift walk. Should recover the arcsine law; that it
#              does is the check that the machinery is honest.
#   drift      + the sample's measured per-bar drift.
#   drift_vol  + each period's own realized volatility.
#   shuffle    the period's OWN sub-period returns, reordered in place. The
#              strongest rung: it conditions on the realized return path and
#              destroys only the sequencing.
#
# Nothing here is monthly. It takes a period key per bar and a position key per bar,
# so months (period=month, position=session), weeks (period=week, position=session)
# and days (period=session, position=bar) all use the same call.

RW_NULL_CONTROLS = ["arcsine", "driftless", "drift", "drift_vol", "shuffle"]
PERIOD_KEY_FUNCS = {
    "session": intraday_trading_day_index,
    "week": trading_week_monday_index,
    "month": trading_month_start_index,
}


def arcsine_null_shares(n_buckets: int) -> list:
    """
    Share of periods whose extreme lands in each equal-width position bucket under a
    driftless random walk, in percent.

    The third arcsine law gives the time of the maximum of a Brownian path on [0, 1]
    the CDF F(t) = 2/pi * arcsin(sqrt t). Differencing it across the bucket edges is
    the whole calculation — it depends only on how many buckets there are, never on
    the data, so it costs nothing to report beside every observed share.
    """
    edges = np.linspace(0.0, 1.0, n_buckets + 1)
    cdf = 2 / np.pi * np.arcsin(np.sqrt(edges))
    return [round(float(v) * 100, 6) for v in np.diff(cdf)]


def _bucket_of(position: np.ndarray, n_positions: int, n_buckets: int) -> np.ndarray:
    """Vectorized `_position_bucket`, returning bucket indexes rather than labels."""
    return np.minimum(position * n_buckets // max(n_positions, 1), n_buckets - 1)


def _null_periods(df: pd.DataFrame, period_index, position_index,
                  drop_edge_periods: bool) -> list:
    """
    Per period: the bar path, its sub-period layout, and where the real extreme fell.

    `Sub_Returns` / `Sub_High_Offset` / `Sub_Low_Offset` describe each sub-period as a
    net log return plus the log distance from its close up to its high and down to its
    low. That triple is exactly sufficient for the shuffle control: which sub-period
    holds the extreme depends only on the running path and those two offsets, never on
    the bar order inside a sub-period.
    """
    period_index = pd.Index(period_index)
    position_index = pd.Index(position_index)
    edges = {period_index.min(), period_index.max()} if drop_edge_periods else set()

    log_high = np.log(df["High"].to_numpy())
    log_low = np.log(df["Low"].to_numpy())
    log_close = np.log(df["Close"].to_numpy())
    log_open = np.log(df["Open"].to_numpy())

    out = []
    for period, positions in pd.Series(np.arange(len(df)), index=period_index).groupby(level=0):
        if period in edges:
            continue
        bars = positions.to_numpy()
        if len(bars) < 2:
            continue
        sub = pd.factorize(position_index[bars])[0]
        n_sub = int(sub.max()) + 1
        if n_sub < 2:
            continue
        # Bar-level path, used by the simulated walks.
        returns = np.diff(log_close[bars], prepend=log_open[bars][0])
        # Sub-period reduction, used by the shuffle control and by the observation.
        last = np.flatnonzero(np.r_[sub[1:] != sub[:-1], True])
        sub_close = log_close[bars][last]
        sub_high = np.maximum.reduceat(log_high[bars], np.r_[0, last[:-1] + 1])
        sub_low = np.minimum.reduceat(log_low[bars], np.r_[0, last[:-1] + 1])
        out.append({
            "Sub_Index": sub,
            "N_Sub": n_sub,
            "Returns": returns,
            "Sigma": float(np.std(returns, ddof=1)) if len(returns) > 1 else 0.0,
            "Sub_Returns": np.diff(sub_close, prepend=log_open[bars][0]),
            "Sub_High_Offset": sub_high - sub_close,
            "Sub_Low_Offset": sub_low - sub_close,
            # The observation comes from the real High/Low series, so it matches what
            # `build_monthly` publishes. Simulated paths carry no intra-bar range, so
            # their extremes are bar-close extremes — on ES the two agree on the third
            # for 187/192 months (high) and 185/192 (low).
            "Observed_High": int(sub[int(np.argmax(log_high[bars]))]),
            "Observed_Low": int(sub[int(np.argmin(log_low[bars]))]),
        })
    return out


def _simulate_null(periods: list, control: str, n_buckets: int, mu: float, sigma: float,
                   n_sim: int, rng) -> tuple[np.ndarray, np.ndarray]:
    """Bucket counts per simulated replica of the whole sample, for high and low."""
    high = np.zeros((n_sim, n_buckets))
    low = np.zeros((n_sim, n_buckets))
    sims = np.arange(n_sim)
    for period in periods:
        n_sub = period["N_Sub"]
        if control == "shuffle":
            # Reorder the period's own sub-period triples; nothing is resampled.
            order = np.argsort(rng.random((n_sim, n_sub)), axis=1)
            path = np.cumsum(period["Sub_Returns"][order], axis=1)
            hi = np.argmax(path + period["Sub_High_Offset"][order], axis=1)
            lo = np.argmin(path + period["Sub_Low_Offset"][order], axis=1)
        else:
            step = {"driftless": 0.0, "drift": mu, "drift_vol": mu}[control]
            scale = period["Sigma"] if control == "drift_vol" else sigma
            path = np.cumsum(rng.normal(step, scale, size=(n_sim, len(period["Returns"]))), axis=1)
            hi = period["Sub_Index"][np.argmax(path, axis=1)]
            lo = period["Sub_Index"][np.argmin(path, axis=1)]
        np.add.at(high, (sims, _bucket_of(hi, n_sub, n_buckets)), 1)
        np.add.at(low, (sims, _bucket_of(lo, n_sub, n_buckets)), 1)
    return high / len(periods) * 100, low / len(periods) * 100


def extreme_position_null(df: pd.DataFrame, period_index, position_index=None,
                          n_buckets: int = 3, labels: list = None, controls: list = None,
                          n_sim: int = RW_NULL_SIMS, seed: int = 42,
                          drop_edge_periods: bool = True,
                          sparse_n: int = SPARSE_MONTHS) -> pd.DataFrame:
    """
    Observed extreme-timing shares against the random-walk null ladder.

    `period_index` is the period key of every bar (a month start, a week Monday, a
    session date); `position_index` is the sub-period the position buckets are
    measured in, defaulting to the bar itself. Both are per-bar arrays, which is what
    makes this timeframe-agnostic — see `PERIOD_KEY_FUNCS`.

    The first and last periods are dropped by default: they are truncated by the
    sample edges and cannot hold a real extreme, matching `complete_months`.

    Returns one row per Event × Bucket × Null. `arcsine` is closed form and carries no
    band or p-value; the simulated rungs carry a 95% band over `n_sim` replicas of the
    whole sample and a two-sided p — the share of replicas at least as extreme as the
    observation. p is a sample-level statement, so it is subject to the same
    multiple-comparisons caveat as every other cell grid here.
    """
    controls = list(controls) if controls is not None else RW_NULL_CONTROLS
    unknown = [c for c in controls if c not in RW_NULL_CONTROLS]
    if unknown:
        raise ValueError(f"Unknown control(s): {', '.join(unknown)}. Known: {', '.join(RW_NULL_CONTROLS)}")
    if labels is None:
        labels = {3: MONTH_THIRDS, 5: MONTH_QUINTILES}.get(n_buckets) or [
            f"B{i + 1}" for i in range(n_buckets)]
    if len(labels) != n_buckets:
        raise ValueError(f"labels has {len(labels)} entries for {n_buckets} buckets")

    columns = ["Event", "Bucket", "n_periods", "observed_pct", "Null", "null_pct",
               "ci_low", "ci_high", "p", "is_sparse"]
    position_index = df.index if position_index is None else position_index
    periods = _null_periods(df, period_index, position_index, drop_edge_periods)
    if not periods:
        return pd.DataFrame(columns=columns)

    n_periods = len(periods)
    observed = {"High": np.zeros(n_buckets), "Low": np.zeros(n_buckets)}
    for period in periods:
        for event in ("High", "Low"):
            bucket = _bucket_of(np.array(period[f"Observed_{event}"]), period["N_Sub"], n_buckets)
            observed[event][int(bucket)] += 1
    observed = {k: v / n_periods * 100 for k, v in observed.items()}

    flat = np.concatenate([p["Returns"] for p in periods])
    mu, sigma = float(flat.mean()), float(flat.std(ddof=1))
    arcsine = arcsine_null_shares(n_buckets)

    records = []
    for control in controls:
        if control == "arcsine":
            draws = None
        else:
            # One generator per control, seeded the same way, so adding or removing a
            # control never shifts another one's numbers.
            draws = _simulate_null(periods, control, n_buckets, mu, sigma, n_sim,
                                   np.random.default_rng(seed))
        for event, event_observed in observed.items():
            samples = None if draws is None else (draws[0] if event == "High" else draws[1])
            for j, label in enumerate(labels):
                if samples is None:
                    null_pct, lo, hi, p = arcsine[j], np.nan, np.nan, np.nan
                else:
                    column = samples[:, j]
                    lo, hi = (round(float(v), 4) for v in np.percentile(column, [2.5, 97.5]))
                    null_pct = round(float(column.mean()), 4)
                    p = round(float(min(2 * min(
                        (column >= event_observed[j]).mean(),
                        (column <= event_observed[j]).mean()), 1.0)), 4)
                records.append({
                    "Event": event,
                    "Bucket": label,
                    "n_periods": n_periods,
                    "observed_pct": round(float(event_observed[j]), 4),
                    "Null": control,
                    "null_pct": null_pct,
                    "ci_low": lo,
                    "ci_high": hi,
                    "p": p,
                    "is_sparse": bool(n_periods < sparse_n),
                })
    return pd.DataFrame.from_records(records)[columns]


# ═══════════════════════════════════════════════════════════════════════════════
# MONTHLY LEVEL RESEARCH
# ═══════════════════════════════════════════════════════════════════════════════

# A monthly level stays live for a whole month, so it cannot be an entry in
# KEY_LEVEL_TIMES, which is keyed by time of day. It gets its own builder.
MONTHLY_LEVELS = ["Monthly_Open", "Prior_Month_High", "Prior_Month_Low", "Prior_Month_Close"]
MONTHLY_LEVEL_COUNT_FROM = (9, 30)


def _monthly_level_eligible_start(index: pd.DatetimeIndex) -> int:
    """
    Position of the first bar at or after 09:30 ET on the month's first RTH session.

    Touch counting cannot start at the 18:00 print that sets `Monthly_Open`: that
    price is made in thin hours and is retested within minutes, so the retap rate
    comes out ~100% and carries no information. This is the monthly analogue of
    `_is_weekly_open_revisit_eligible`, which starts weekly-open counting at Monday
    09:30 for the same reason. Prior-month levels are known before the month opens
    and use the same guard for comparability.

    The window is bounded above by 18:00 so the evening reopen — which shares a
    session date with the RTH hours that follow it — cannot qualify.
    """
    sessions = intraday_trading_day_index(index)
    minute_of_day = np.asarray(index.hour) * 60 + np.asarray(index.minute)
    start_minute = MONTHLY_LEVEL_COUNT_FROM[0] * 60 + MONTHLY_LEVEL_COUNT_FROM[1]
    rth = (minute_of_day >= start_minute) & (minute_of_day < 18 * 60)
    # An RTH session is one holding a bar at or after 09:30; a holiday can leave the
    # month's first session date without one.
    for session in pd.DatetimeIndex(pd.unique(sessions)):
        candidates = np.flatnonzero((sessions == session) & rth)
        if len(candidates):
            return int(candidates[0])
    return len(index)


def _month_level_context(m: pd.DataFrame) -> tuple:
    """Index, ordered session dates, per-bar session index, and the eligible start."""
    idx = m.index
    sessions = intraday_trading_day_index(idx)
    ordered = pd.DatetimeIndex(pd.unique(sessions))
    return idx, ordered, ordered.get_indexer(sessions), _monthly_level_eligible_start(idx)


def _monthly_level_values(m: pd.DataFrame, prev: dict) -> dict:
    """The four monthly levels for this month; prior-month entries are NaN for month one."""
    return {
        "Monthly_Open": float(m["Open"].iloc[0]),
        "Prior_Month_High": prev["Monthly_High"] if prev else np.nan,
        "Prior_Month_Low": prev["Monthly_Low"] if prev else np.nan,
        "Prior_Month_Close": prev["Monthly_Close"] if prev else np.nan,
    }


def build_monthly_level_rows(df_1m: pd.DataFrame) -> pd.DataFrame:
    """One row per session month × monthly level with touch and excursion context."""
    records = []
    month_keys = trading_month_start_index(df_1m.index)
    first_month, last_month = month_keys.min(), month_keys.max()
    prev = None
    for month_start, m in df_1m.groupby(month_keys, sort=True):
        m = m.sort_index()
        current = {
            "Monthly_High": float(m["High"].max()),
            "Monthly_Low": float(m["Low"].min()),
            "Monthly_Close": float(m["Close"].iloc[-1]),
        }
        idx, ordered, session_index, start_pos = _month_level_context(m)
        if len(m) >= 2 and start_pos < len(idx):
            n_sessions = len(ordered)
            lows = m["Low"].to_numpy()
            highs = m["High"].to_numpy()
            values = _monthly_level_values(m, prev)
            for level_name in MONTHLY_LEVELS:
                value = values[level_name]
                if pd.isna(value):
                    continue
                touch = (lows <= value) & (highs >= value)
                touch[:start_pos] = False
                touched = bool(touch.any())
                rec = {
                    "Month_Start": month_start,
                    "Month_Of_Year": int(month_start.month),
                    "N_Sessions": n_sessions,
                    "Is_Partial": bool(month_start == first_month or month_start == last_month),
                    "Level_Name": level_name,
                    "Level_Value": value,
                    "Touched": touched,
                    "Touch_Bars_Total": int(touch.sum()),
                    "Touch_Sessions": int(len(np.unique(session_index[touch]))),
                    "Month_Close_Above_Level": bool(current["Monthly_Close"] > value),
                }
                if touched:
                    first_pos = int(np.flatnonzero(touch)[0])
                    first_ts = idx[first_pos]
                    first_index = int(session_index[first_pos])
                    rec.update({
                        "First_Touch_Timestamp": first_ts,
                        "First_Touch_Session_Index": first_index,
                        "First_Touch_Session_Pos": round(first_index / (n_sessions - 1), 6) if n_sessions > 1 else 0.0,
                        "First_Touch_Third": _position_bucket(first_index, n_sessions, MONTH_THIRDS),
                        "First_Touch_Weekday": trading_weekday(first_ts),
                        "First_Touch_Session": session_of(first_ts),
                        "Post_Touch_High_Excursion": float(highs[first_pos:].max() - value),
                        "Post_Touch_Low_Excursion": float(value - lows[first_pos:].min()),
                    })
                else:
                    rec.update({
                        "First_Touch_Timestamp": pd.NaT,
                        "First_Touch_Session_Index": np.nan,
                        "First_Touch_Session_Pos": np.nan,
                        "First_Touch_Third": np.nan,
                        "First_Touch_Weekday": np.nan,
                        "First_Touch_Session": np.nan,
                        "Post_Touch_High_Excursion": np.nan,
                        "Post_Touch_Low_Excursion": np.nan,
                    })
                records.append(rec)
        prev = current
    return pd.DataFrame.from_records(records)


def monthly_level_touch_distribution(rows: pd.DataFrame,
                                     sparse_months: int = SPARSE_MONTHS) -> pd.DataFrame:
    """Touch rate, touch sessions and touch bars per monthly level, over complete months."""
    clean = rows[~rows["Is_Partial"].astype(bool)]
    records = []
    for level_name, group in clean.groupby("Level_Name", dropna=False):
        stats = bootstrap_probability_ci(group["Touched"])
        records.append({
            "Level_Name": level_name,
            "months": int(len(group)),
            "n": int(group["Touched"].sum()),
            "pct": stats["probability"],
            "ci_low": stats["ci_low"],
            "ci_high": stats["ci_high"],
            "is_sparse": bool(len(group) < sparse_months),
            "avg_touch_sessions": round(float(group["Touch_Sessions"].mean()), 4),
            "median_touch_sessions": round(float(group["Touch_Sessions"].median()), 4),
            "avg_touch_bars": round(float(group["Touch_Bars_Total"].mean()), 4),
            "month_close_above_level_pct": round(float(group["Month_Close_Above_Level"].mean() * 100), 4),
        })
    return pd.DataFrame.from_records(records)


def monthly_level_first_touch_by_third(rows: pd.DataFrame,
                                       sparse_months: int = SPARSE_MONTHS) -> pd.DataFrame:
    """Share of touched complete months whose first touch fell in each session third."""
    clean = rows[(~rows["Is_Partial"].astype(bool)) & rows["Touched"].astype(bool)]
    records = []
    for level_name, group in clean.groupby("Level_Name", dropna=False):
        for third in MONTH_THIRDS:
            hits = group["First_Touch_Third"].eq(third)
            stats = bootstrap_probability_ci(hits)
            records.append({
                "Level_Name": level_name,
                "First_Touch_Third": third,
                "months": int(len(group)),
                "n": int(hits.sum()),
                "pct": stats["probability"],
                "ci_low": stats["ci_low"],
                "ci_high": stats["ci_high"],
                "is_sparse": bool(int(hits.sum()) < sparse_months),
            })
    return pd.DataFrame.from_records(records)


def monthly_level_forward_touch(df_1m: pd.DataFrame, n_deciles: int = 10,
                                sparse_months: int = SPARSE_MONTHS) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    P(level touched from the start of session k through month end), on two axes.

    Raw session index is directly interpretable, but n falls away past ~19 sessions
    because short months stop contributing — the count column records that. The
    normalized decile axis gives every month a contribution to every bucket, so the
    curve is comparable across month lengths.

    Reuses the reverse `np.maximum.accumulate` trick from
    `intraday_level_forward_touch_distribution`, applied per month. Partial months
    are excluded, and counting starts at the same 09:30 guard the row builder uses.
    """
    by_index, by_decile = {}, {}
    month_keys = trading_month_start_index(df_1m.index)
    first_month, last_month = month_keys.min(), month_keys.max()
    prev = None
    for month_start, m in df_1m.groupby(month_keys, sort=True):
        m = m.sort_index()
        current = {
            "Monthly_High": float(m["High"].max()),
            "Monthly_Low": float(m["Low"].min()),
            "Monthly_Close": float(m["Close"].iloc[-1]),
        }
        idx, ordered, session_index, start_pos = _month_level_context(m)
        partial = month_start == first_month or month_start == last_month
        if len(m) >= 2 and start_pos < len(idx) and not partial:
            n_sessions = len(ordered)
            lows = m["Low"].to_numpy()
            highs = m["High"].to_numpy()
            # Sessions are contiguous in a sorted index, so the first eligible bar of
            # each session is the first position where the session index changes.
            tail = session_index[start_pos:]
            changes = np.flatnonzero(np.r_[True, tail[1:] != tail[:-1]]) + start_pos
            first_eligible = {int(session_index[p]): int(p) for p in changes}
            values = _monthly_level_values(m, prev)
            for level_name in MONTHLY_LEVELS:
                value = values[level_name]
                if pd.isna(value):
                    continue
                touch = (lows <= value) & (highs >= value)
                touch[:start_pos] = False
                future = np.maximum.accumulate(touch[::-1])[::-1]
                for session_k, pos in first_eligible.items():
                    by_index.setdefault((level_name, session_k), []).append(bool(future[pos]))
                for decile in range(n_deciles):
                    session_k = int(decile * n_sessions / n_deciles)
                    pos = first_eligible.get(session_k)
                    if pos is None:
                        continue
                    by_decile.setdefault((level_name, decile), []).append(bool(future[pos]))
        prev = current

    def _frame(agg: dict, key_name: str, label) -> pd.DataFrame:
        columns = ["Level_Name", key_name, "n_months", "touch_months", "touch_pct",
                   "ci_low", "ci_high", "is_sparse"]
        records = []
        for (level_name, key), hits in sorted(agg.items(), key=lambda kv: (kv[0][0], kv[0][1])):
            stats = bootstrap_probability_ci(pd.Series(hits))
            records.append({
                "Level_Name": level_name,
                key_name: label(key),
                "n_months": len(hits),
                "touch_months": int(sum(hits)),
                "touch_pct": stats["probability"],
                "ci_low": stats["ci_low"],
                "ci_high": stats["ci_high"],
                "is_sparse": bool(len(hits) < sparse_months),
            })
        return pd.DataFrame.from_records(records) if records else pd.DataFrame(columns=columns)

    return (
        _frame(by_index, "Session_Index", lambda k: k),
        _frame(by_decile, "Decile", lambda d: f"D{d + 1}"),
    )


# ───────────────────────────────────────────────────────────────────────────────
# LEVEL TOUCH NULL
#
# "Prior_Month_High is touched in 69% of months, Prior_Month_Low in 31%" reads as a
# statement about levels. It is mostly a statement about DRIFT. In an upward-drifting
# series the level sitting above spot is reached far more often than the one below,
# with no level-specific behaviour involved at all.
#
# The extreme-timing work already carries a ladder that prices this in. Touch rate had
# none, so this is the analogue — same controls, same seeding discipline, same
# sample-level p. `arcsine` is absent: it is a closed form for the TIME OF THE MAXIMUM
# and says nothing about whether a fixed price is reached.
#
# The statistic is P(level touched between the 09:30 eligibility guard and month end),
# so every rung is simulated from the same starting bar the observation uses, with the
# level held at its real log distance from that bar's open.
MONTHLY_LEVEL_NULL_CONTROLS = ["driftless", "drift", "drift_vol", "shuffle"]
# Simulated paths step hourly, not every minute. Whether a walk reaches a level is a
# property of the CONTINUOUS path's running extreme, and 1-minute discretization
# approximates that no better than hourly while costing 60x the draws — a month holds
# ~30,000 minute bars against ~500 hourly ones. The observation is still measured on
# the full 1-minute series; only the simulation is strided.
MONTHLY_LEVEL_NULL_STRIDE = 60


def _level_null_months(df_1m: pd.DataFrame, stride: int = MONTHLY_LEVEL_NULL_STRIDE) -> list:
    """
    Per complete month: the eligible path, its session reduction, and each level's
    log distance from the first eligible bar.

    Distances are logs from one fixed reference — the open of the first eligible bar —
    so a simulated path starting at 0 is directly comparable to the real one, and the
    level values never have to be re-derived in price space.
    """
    months = []
    month_keys = trading_month_start_index(df_1m.index)
    first_month, last_month = month_keys.min(), month_keys.max()
    prev = None
    for month_start, m in df_1m.groupby(month_keys, sort=True):
        m = m.sort_index()
        current = {
            "Monthly_High": float(m["High"].max()),
            "Monthly_Low": float(m["Low"].min()),
            "Monthly_Close": float(m["Close"].iloc[-1]),
        }
        idx, ordered, session_index, start_pos = _month_level_context(m)
        partial = month_start == first_month or month_start == last_month
        if len(m) >= 2 and start_pos < len(idx) - 1 and not partial:
            lows = m["Low"].to_numpy()[start_pos:]
            highs = m["High"].to_numpy()[start_pos:]
            closes = m["Close"].to_numpy()[start_pos:]
            opens = m["Open"].to_numpy()[start_pos:]
            sub = session_index[start_pos:] - session_index[start_pos]
            n_sub = int(sub.max()) + 1
            if n_sub >= 2:
                ref = float(opens[0])
                log_close = np.log(closes)
                # Strided for the simulated rungs; see MONTHLY_LEVEL_NULL_STRIDE. The
                # final close is kept so the path always spans the whole month.
                keep = np.union1d(np.arange(0, len(log_close), stride), [len(log_close) - 1])
                returns = np.diff(log_close[keep], prepend=np.log(ref))
                # Session reduction, as in `_null_periods`: the shuffle control needs
                # each session's net return plus its high/low offsets, and nothing else.
                last = np.flatnonzero(np.r_[sub[1:] != sub[:-1], True])
                sub_close = log_close[last]
                sub_high = np.log(np.maximum.reduceat(highs, np.r_[0, last[:-1] + 1]))
                sub_low = np.log(np.minimum.reduceat(lows, np.r_[0, last[:-1] + 1]))
                values = _monthly_level_values(m, prev)
                levels = {}
                for level_name in MONTHLY_LEVELS:
                    value = values[level_name]
                    if pd.isna(value) or value <= 0:
                        continue
                    touch = (lows <= value) & (highs >= value)
                    levels[level_name] = {
                        "Distance": float(np.log(value) - np.log(ref)),
                        "Observed": bool(touch.any()),
                    }
                if levels:
                    months.append({
                        "N_Sub": n_sub,
                        "Returns": returns,
                        "Sigma": float(np.std(returns, ddof=1)) if len(returns) > 1 else 0.0,
                        "Sub_Returns": np.diff(sub_close, prepend=np.log(ref)),
                        "Sub_High_Offset": sub_high - sub_close,
                        "Sub_Low_Offset": sub_low - sub_close,
                        "Levels": levels,
                    })
        prev = current
    return months


def _simulate_level_touch(months: list, control: str, level_name: str, mu: float,
                          sigma: float, n_sim: int, rng) -> np.ndarray:
    """
    Touch rate per simulated replica of the whole sample, for one level.

    The simulated walks carry no intra-bar range, so a level is counted as touched when
    the bar-close path reaches its distance: `max(path) >= d` above spot, `min(path)
    <= d` below. `shuffle` keeps the month's own session highs and lows, so it tests
    containment — `low_i <= d <= high_i` for some session — which is exactly the real
    test. That asymmetry makes `shuffle` the strictest rung, which is the point of it.
    """
    hits = np.zeros(n_sim)
    n_months = 0
    for month in months:
        level = month["Levels"].get(level_name)
        if level is None:
            continue
        n_months += 1
        d = level["Distance"]
        if control == "shuffle":
            order = np.argsort(rng.random((n_sim, month["N_Sub"])), axis=1)
            path = np.cumsum(month["Sub_Returns"][order], axis=1)
            hi = path + month["Sub_High_Offset"][order]
            lo = path + month["Sub_Low_Offset"][order]
            hits += ((lo <= d) & (hi >= d)).any(axis=1)
        else:
            step = {"driftless": 0.0, "drift": mu, "drift_vol": mu}[control]
            scale = month["Sigma"] if control == "drift_vol" else sigma
            path = np.cumsum(rng.normal(step, scale, size=(n_sim, len(month["Returns"]))), axis=1)
            hits += (path.max(axis=1) >= d) if d >= 0 else (path.min(axis=1) <= d)
    return hits / max(n_months, 1) * 100


def monthly_level_touch_null(df_1m: pd.DataFrame, controls: list = None,
                             n_sim: int = RW_NULL_SIMS, seed: int = 42,
                             sparse_months: int = SPARSE_MONTHS) -> pd.DataFrame:
    """
    Observed monthly level touch rate against the random-walk null ladder.

    One row per Level_Name × Null. `p` is two-sided over `n_sim` replicas of the whole
    sample and is a sample-level statement, subject to the same multiple-comparisons
    caveat as every other cell grid here.
    """
    controls = list(controls) if controls is not None else MONTHLY_LEVEL_NULL_CONTROLS
    unknown = [c for c in controls if c not in MONTHLY_LEVEL_NULL_CONTROLS]
    if unknown:
        raise ValueError(
            f"Unknown control(s): {', '.join(unknown)}. "
            f"Known: {', '.join(MONTHLY_LEVEL_NULL_CONTROLS)}")

    columns = ["Level_Name", "n_months", "observed_pct", "Null", "null_pct",
               "ci_low", "ci_high", "p", "is_sparse"]
    months = _level_null_months(df_1m)
    if not months:
        return pd.DataFrame(columns=columns)

    flat = np.concatenate([m["Returns"] for m in months])
    mu, sigma = float(flat.mean()), float(flat.std(ddof=1))

    records = []
    for level_name in MONTHLY_LEVELS:
        present = [m for m in months if level_name in m["Levels"]]
        if not present:
            continue
        observed = float(np.mean([m["Levels"][level_name]["Observed"] for m in present]) * 100)
        for control in controls:
            # One generator per control, seeded the same way, so adding or removing a
            # control never shifts another one's numbers.
            column = _simulate_level_touch(present, control, level_name, mu, sigma,
                                           n_sim, np.random.default_rng(seed))
            lo, hi = (round(float(v), 4) for v in np.percentile(column, [2.5, 97.5]))
            records.append({
                "Level_Name": level_name,
                "n_months": len(present),
                "observed_pct": round(observed, 4),
                "Null": control,
                "null_pct": round(float(column.mean()), 4),
                "ci_low": lo,
                "ci_high": hi,
                "p": round(float(min(2 * min((column >= observed).mean(),
                                             (column <= observed).mean()), 1.0)), 4),
                "is_sparse": bool(len(present) < sparse_months),
            })
    return pd.DataFrame.from_records(records)[columns]


def run_monthly_levels_research(symbol: str = None, path: str = DATA_PATH,
                                output_dir: Path = None) -> pd.DataFrame:
    """
    Monthly level touch research from source 1m data.

    Prior-month levels come from the 1-minute frame itself, not from `build_monthly`
    on a resampled frame, so the level values match the bars they are tested against.
    """
    symbol = symbol or SYMBOL
    out_dir = output_dir or module_output_dir("monthly_levels", symbol)
    print(f"[Research] {symbol} monthly levels from 1m source ...")
    df_1m = load_1m_source(path)

    rows = build_monthly_level_rows(df_1m)
    _write_csv(rows, "monthly_level_rows", output_dir=out_dir)

    distribution = monthly_level_touch_distribution(rows)
    _write_csv(distribution, "monthly_level_touch_distribution", output_dir=out_dir)
    _write_csv(monthly_level_first_touch_by_third(rows),
               "monthly_level_first_touch_by_third", output_dir=out_dir)

    by_index, by_decile = monthly_level_forward_touch(df_1m)
    _write_csv(by_index, "monthly_level_forward_touch_by_session_index", output_dir=out_dir)
    _write_csv(by_decile, "monthly_level_forward_touch_by_decile", output_dir=out_dir)

    touch_null = monthly_level_touch_null(df_1m)
    _write_csv(touch_null, "monthly_level_touch_null", output_dir=out_dir)

    print("\n[Research] Monthly level touch distribution:")
    print(distribution.to_string(index=False))
    print("\n[Research] Monthly level touch vs the null ladder:")
    print(touch_null.to_string(index=False))
    return rows


# ═══════════════════════════════════════════════════════════════════════════════
# MONTH-STATE CONTEXT
# ═══════════════════════════════════════════════════════════════════════════════

# THE KNOWABILITY RULE
# A month-state conditioner must be knowable at the time of the row it conditions.
# Prior-month features and month-to-date path features qualify. The month's eventual
# direction, high, low or close do not: conditioning a daily outcome on the whole
# month's label repeats the contemporaneous-label defect removed from the weekly
# targets. This allow-list is the enforcement point, guarded by a test.
MONTH_STATE_CONDITIONERS = [
    "Month_Of_Year",
    "Session_Pos_In_Month",
    "Third_In_Month",
    "Quintile_In_Month",
    "Week_Of_Month",
    "Month_Return_So_Far_Pct",
    "Month_Return_To_Prior_Close_Pct",
    "Above_Monthly_Open",
    "Prior_Month_Bull_Bear",
]
MONTH_STATE_NUMERIC_CONDITIONERS = [
    "Session_Pos_In_Month",
    "Month_Return_So_Far_Pct",
    "Month_Return_To_Prior_Close_Pct",
]
# Every `build_monthly` column that describes the FINISHED month. Targets, never
# conditioners. `Monthly_Open` is deliberately absent: it is set by the month's
# first bar and is knowable at every session inside the month.
WHOLE_MONTH_LABELS = [
    "Bull_Bear", "Monthly_Close", "Monthly_High", "Monthly_Low",
    "High_Session_Index", "Low_Session_Index", "High_Session_Pos", "Low_Session_Pos",
    "High_Third", "Low_Third", "High_Quintile", "Low_Quintile",
    "High_Week_Of_Month", "Low_Week_Of_Month", "High_Weekday", "Low_Weekday",
    "High_Session", "Low_Session", "High_Hour", "Low_Hour",
    "High_Day_Of_Month", "Low_Day_Of_Month",
]
MONTHLY_TARGETS = ["Month_Bullish", "Monthly_High_Late", "Monthly_Low_Early"]


def is_whole_month_label(column: str) -> bool:
    """True when `column` describes the completed month rather than its path so far."""
    return column in WHOLE_MONTH_LABELS


def build_month_state_by_session(df: pd.DataFrame, monthly: pd.DataFrame) -> pd.DataFrame:
    """
    One row per session carrying the month-state conditioners for that session.

    `N_Sessions` comes from the exchange calendar, not from price. It is published
    in advance, so normalized session position is knowable at session 1 of the month
    and does not breach the knowability rule.

    `Month_Return_So_Far_Pct` runs monthly open → this session's close, so against a
    same-session target it is contemporaneous. `Month_Return_To_Prior_Close_Pct` is
    the fully lagged twin: monthly open → the previous session's close, knowable at
    this session's open. Both are reported.
    """
    session_keys = intraday_trading_day_index(df.index)
    closes = df.groupby(session_keys)["Close"].last()
    session_dates = pd.DatetimeIndex(pd.unique(session_keys))
    frame = pd.DataFrame({
        "Trading_Day": session_dates,
        "Month_Start": trading_month_start_index(session_dates),
    })
    frame["Session_Index_In_Month"] = frame.groupby("Month_Start").cumcount()
    frame["N_Sessions"] = frame.groupby("Month_Start")["Trading_Day"].transform("size")
    frame["Month_Of_Year"] = frame["Month_Start"].dt.month
    frame["Session_Pos_In_Month"] = np.where(
        frame["N_Sessions"] > 1,
        frame["Session_Index_In_Month"] / (frame["N_Sessions"] - 1),
        0.0,
    )
    frame["Third_In_Month"] = [
        _position_bucket(i, n, MONTH_THIRDS)
        for i, n in zip(frame["Session_Index_In_Month"], frame["N_Sessions"])
    ]
    frame["Quintile_In_Month"] = [
        _position_bucket(i, n, MONTH_QUINTILES)
        for i, n in zip(frame["Session_Index_In_Month"], frame["N_Sessions"])
    ]
    frame["Week_Of_Month"] = [week_of_month(d) for d in frame["Trading_Day"]]

    labels = monthly.set_index("Month_Start") if not monthly.empty else pd.DataFrame()
    session_close = frame["Trading_Day"].map(closes)
    monthly_open = frame["Month_Start"].map(
        labels["Monthly_Open"] if "Monthly_Open" in labels else pd.Series(dtype=float))
    frame["Month_Return_So_Far_Pct"] = (session_close / monthly_open - 1) * 100
    frame["Above_Monthly_Open"] = session_close > monthly_open
    frame["Month_Return_To_Prior_Close_Pct"] = (
        frame.groupby("Month_Start")["Month_Return_So_Far_Pct"].shift(1))
    frame["Prior_Month_Bull_Bear"] = frame["Month_Start"].map(
        labels["Prev_Bull_Bear"] if "Prev_Bull_Bear" in labels else pd.Series(dtype=object))
    return frame


def build_week_month_state(session_state: pd.DataFrame) -> pd.DataFrame:
    """
    Month-state for a trading week, taken from its first session (the Monday).

    Roughly a third of trading weeks span two session months, so the flag matters:
    the Monday's month-state does not describe the whole week.
    """
    state = session_state.copy()
    state["Week_Start"] = trading_week_monday_index(pd.DatetimeIndex(state["Trading_Day"]))
    straddles = (
        state.groupby("Week_Start")["Month_Start"].nunique().gt(1)
        .rename("Straddles_Month_Boundary").reset_index()
    )
    # `drop_duplicates`, not `groupby(...).first()`: the latter takes the first
    # NON-NULL value per column, so a week whose Monday opens a month would borrow
    # `Month_Return_To_Prior_Close_Pct` from a later session instead of reporting the
    # NaN that says the value does not exist yet.
    monday = state.sort_values("Trading_Day").drop_duplicates("Week_Start", keep="first")
    return monday.merge(straddles, on="Week_Start", how="left")


def attach_month_state(rows: pd.DataFrame, session_state: pd.DataFrame,
                       day_col: str = "Trading_Day") -> pd.DataFrame:
    """Join the session-level month-state conditioners onto a day-level row frame."""
    keep = ["Trading_Day", "Month_Start", "Session_Index_In_Month", "N_Sessions"]
    keep += [c for c in MONTH_STATE_CONDITIONERS if c not in keep]
    state = session_state[keep].rename(columns={"Trading_Day": day_col})
    out = rows.copy()
    out[day_col] = pd.to_datetime(out[day_col])
    return out.merge(state, on=day_col, how="left")


def attach_week_month_state(rows: pd.DataFrame, week_state: pd.DataFrame,
                            week_col: str = "Week_Start") -> pd.DataFrame:
    """
    Join month-state onto weekly rows, taken from the week's Monday session.

    `Straddles_Month_Boundary` rides along: about a third of trading weeks span two
    session months, and for those the Monday's state describes only part of the week.
    """
    keep = ["Week_Start", "Month_Start", "Session_Index_In_Month", "N_Sessions",
            "Straddles_Month_Boundary"]
    keep += [c for c in MONTH_STATE_CONDITIONERS if c not in keep]
    state = week_state[keep].rename(columns={"Week_Start": week_col})
    out = rows.copy()
    out[week_col] = pd.to_datetime(out[week_col])
    return out.merge(state, on=week_col, how="left")


def attach_monthly_targets(rows: pd.DataFrame, monthly: pd.DataFrame) -> pd.DataFrame:
    """
    Attach whole-month labels as TARGETS. They are never conditioners.

    Only complete months contribute, so rows in the sample's truncated edge months
    carry NaN targets and drop out of the monthly summaries.
    """
    labels = complete_months(monthly)
    if labels.empty:
        for target in MONTHLY_TARGETS:
            rows = rows.assign(**{target: np.nan})
        return rows
    labels = labels[["Month_Start", "Bull_Bear", "High_Third", "Low_Third"]].copy()
    labels["Month_Bullish"] = labels["Bull_Bear"].eq("Bullish")
    labels["Monthly_High_Late"] = labels["High_Third"].eq("Late")
    labels["Monthly_Low_Early"] = labels["Low_Third"].eq("Early")
    return rows.merge(labels[["Month_Start"] + MONTHLY_TARGETS], on="Month_Start", how="left")


def apply_month_state_buckets(rows: pd.DataFrame, conditioners: list = None) -> pd.DataFrame:
    """Bucket numeric month-state conditioners on train p25/p75, as every other feature is."""
    conditioners = conditioners or MONTH_STATE_NUMERIC_CONDITIONERS
    out = rows.copy()
    for feature in conditioners:
        if feature not in out:
            continue
        bucket_col = f"{feature}_Bucket"
        out[bucket_col] = "Middle 25-75%"
        vals = out[out["Split"].eq("Train")][feature].dropna()
        if vals.empty:
            continue
        q25, q75 = vals.quantile([0.25, 0.75])
        out.loc[out[feature] <= q25, bucket_col] = "P25 Low"
        out.loc[out[feature] >= q75, bucket_col] = "P75 High"
        out.loc[out[feature].isna(), bucket_col] = np.nan
    return out


def month_state_conditioner_columns(rows: pd.DataFrame) -> list:
    """Allow-listed conditioner names as they appear on a bucketed frame."""
    names = [
        f"{c}_Bucket" if c in MONTH_STATE_NUMERIC_CONDITIONERS else c
        for c in MONTH_STATE_CONDITIONERS
    ]
    return [name for name in names if name in rows]


def month_state_outcomes(rows: pd.DataFrame, targets: list, group_cols: list = None,
                         extra_conditioners: list = None,
                         monthly_targets: list = None) -> pd.DataFrame:
    """
    Long-format outcome table by month-state conditioner.

    THE SCOPE RULE FOR MONTHLY TARGETS
    Row-level targets are scored on every row. Monthly targets are scored on
    `Third_In_Month == "Early"` rows only and carry `Month_Scope = "Early"`: a
    late-month row "predicting" its own month's close is describing it. This is the
    session-position generalization of the weekday scope rule already applied to
    weekly targets in `_build_twap_vwap_summary` and
    `intraday_to_weekly_path_dependency`.

    `n_months` sits beside `n` for the same reason `n_weeks` does: one monthly label
    repeats across up to 24 day-rows, so `n` overstates independent observations.
    """
    group_cols = list(group_cols) if group_cols else ["Split"]
    monthly_targets = list(monthly_targets) if monthly_targets is not None else MONTHLY_TARGETS
    columns = group_cols + ["Month_Scope", "Conditioner", "Conditioner_Value", "Target",
                            "n", "n_months", "pct"]
    detail = apply_month_state_buckets(rows)
    conditioners = month_state_conditioner_columns(detail)
    conditioners += [c for c in (extra_conditioners or []) if c in detail]
    scopes = [
        ("All", detail, [t for t in targets if t in detail]),
        ("Early", detail[detail["Third_In_Month"].eq("Early")],
         [t for t in monthly_targets if t in detail]),
    ]

    records = []
    for scope, scope_rows, scope_targets in scopes:
        if scope_rows.empty or not scope_targets:
            continue
        for conditioner in conditioners:
            for keys, group in scope_rows.groupby(group_cols + [conditioner], dropna=False):
                keys = keys if isinstance(keys, tuple) else (keys,)
                base = dict(zip(group_cols, keys[:-1]))
                for target in scope_targets:
                    values = group[target].dropna()
                    # A cell with no observation of this target is not a 0% result,
                    # it is silence. Partial edge months carry NaN monthly targets and
                    # would otherwise emit n=0 rows across every conditioner.
                    if values.empty:
                        continue
                    records.append({
                        **base,
                        "Month_Scope": scope,
                        "Conditioner": conditioner,
                        "Conditioner_Value": keys[-1],
                        "Target": target,
                        "n": int(len(values)),
                        "n_months": int(group.loc[values.index, "Month_Start"].nunique()),
                        "pct": _pct(values),
                    })
    if not records:
        return pd.DataFrame(columns=columns)
    out = pd.DataFrame.from_records(records)
    # Conditioner values mix ints, bools and strings across conditioners; sorting the
    # mixed column raises unless it is cast first.
    out["Conditioner_Value"] = out["Conditioner_Value"].astype(str)
    sort_cols = group_cols + ["Month_Scope", "Conditioner", "Conditioner_Value", "Target"]
    return out.sort_values(sort_cols).reset_index(drop=True)


def run_month_context_research(symbol: str = None, path: str = DATA_PATH,
                               output_dir: Path = None,
                               resample_to: str = None) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Month-state as a conditioner on the existing intraday and weekly rows.

    Adds columns to the existing row builders and writes new summary files. It does
    not alter any existing figure.
    """
    symbol = symbol or SYMBOL
    out_dir = output_dir or module_output_dir("month_context", symbol)
    print(f"[Research] {symbol} month context ...")
    df_1m = load_1m_source(path)
    df = load_and_resample(path, resample_to or RESAMPLE_TO)
    monthly = build_monthly(df)
    weekly = build_weekly(df)
    session_state = build_month_state_by_session(df, monthly)

    key_level = attach_monthly_targets(
        attach_month_state(build_intraday_key_level_rows(df_1m), session_state), monthly)
    key_level["Row_Source"] = "intraday_levels"
    key_level["Window_Name"] = "Day"

    relative = attach_monthly_targets(
        attach_month_state(
            drop_lookahead_level_windows(build_relative_level_path_rows(df_1m)), session_state),
        monthly)
    relative["Row_Source"] = "relative_path"

    group_cols = ["Row_Source", "Split", "Level_Name", "Window_Name"]
    intraday = pd.concat([
        month_state_outcomes(key_level, ["Day_Bullish", "Day_Close_Above_Level"], group_cols=group_cols),
        month_state_outcomes(relative, ["Day_Bullish", "Next_Session_Bullish"], group_cols=group_cols),
    ], ignore_index=True)
    _write_csv(intraday, "intraday_outcomes_by_month_state", output_dir=out_dir)

    week_state = build_week_month_state(session_state)
    weekly_rows = weekly.copy()
    weekly_rows.index.name = "Week_Start"
    weekly_rows = weekly_rows.reset_index()
    weekly_rows["Split"] = np.where(weekly_rows["Week_Start"] <= TRAIN_END, "Train", "OOS")
    weekly_rows["Week_Bullish"] = weekly_rows["Bull_Bear"].eq("Bullish")
    weekly_rows["Weekly_High_Friday"] = weekly_rows["High_Weekday"].eq("Friday")
    weekly_rows["Weekly_Low_Monday"] = weekly_rows["Low_Weekday"].eq("Monday")
    weekly_rows = attach_monthly_targets(
        attach_week_month_state(weekly_rows, week_state), monthly)
    weekly_summary = month_state_outcomes(
        weekly_rows,
        ["Week_Bullish", "Weekly_High_Friday", "Weekly_Low_Monday"],
        group_cols=["Split"],
        extra_conditioners=["Straddles_Month_Boundary"],
    )
    _write_csv(weekly_summary, "weekly_outcomes_by_month_state", output_dir=out_dir)
    return intraday, weekly_summary


# ═══════════════════════════════════════════════════════════════════════════════
# WEEK-STATE → INTRADAY WINDOW CONTEXT
# ═══════════════════════════════════════════════════════════════════════════════
#
# Does higher-timeframe path state condition the path of a LOWER timeframe — not the
# day's closing direction, which `month_context` already answers in the negative, but
# the individual windows inside the day, and their MAGNITUDE as well as their sign.
#
# Two conditioner families, both strictly lagged relative to the window being scored:
#
#   week state   the week's path up to the END OF THE PREVIOUS SESSION. Never the
#                current session, so a Wednesday window is never conditioned on
#                Wednesday's own move.
#   day state    the current session's path from its open up to the WINDOW OPEN. This
#                is the "day's own prior path" rung: legitimate for a 13:00 window
#                because 09:30–13:00 has already happened, and unusable for the
#                Globex window, which has no prior path inside its session.
#
# There is deliberately NO contemporaneous variant of either. `month_context` shipped
# `Month_Return_So_Far_Pct` and `Above_Monthly_Open` measured to the current session's
# close, and they produce a ~28pp spread against a same-session target that is simply
# the session's own return read back. Nothing here is measured past the window open.
#
# THE VOLATILITY-CLUSTERING BASELINE
#
# Any magnitude target conditioned on any volatility-flavoured state will "work",
# because volatility clusters. That is GARCH, not path structure, and a raw range
# table cannot tell the two apart. So every magnitude target is reported twice:
#
#   Window_Range_Pct        raw, in percent of the window open
#   Window_Log_Range_Ratio  log(today's range / the PRIOR DAY's same-window range)
#
# A conditioner that only rediscovers vol clustering moves the raw column and leaves
# the ratio flat. Only a conditioner that moves the ratio carries information the
# previous day's range did not already have. The ratio is the finding; the raw column
# is there so the ratio's denominator is auditable.
#
# The ratio is reported in LOGS, not levels. A raw range ratio is bounded below by 0
# and unbounded above — on ES it runs median 0.99, mean 1.19, max 11.4 — so a mean of
# it is dragged by the right tail and overstates every bucket, the widest bucket most.
# log(today/yesterday) is symmetric about 0: -0.3 and +0.3 are the same size move in
# opposite directions, and exp(spread) reads directly as a multiplicative factor.
# `Window_Range_Ratio` stays on the row detail as the auditable level.

WEEK_STATE_CONDITIONERS = [
    "Session_Index_In_Week",
    "Week_Return_To_Prior_Close_Pct",
    "Week_Range_So_Far_Pct",
    "Above_Weekly_Open_At_Prior_Close",
    "Prior_Week_Bull_Bear",
]
DAY_PRIOR_CONDITIONERS = [
    "Day_Return_To_Window_Open_Pct",
    "Day_Range_To_Window_Open_Pct",
    "Prior_Window_Return_Pct",
    "Prior_Window_Range_Pct",
]
WEEK_CONTEXT_NUMERIC_CONDITIONERS = [
    "Week_Return_To_Prior_Close_Pct",
    "Week_Range_So_Far_Pct",
    "Day_Return_To_Window_Open_Pct",
    "Day_Range_To_Window_Open_Pct",
    "Prior_Window_Return_Pct",
    "Prior_Window_Range_Pct",
]
# `Window_Range_Ratio` divides by the PRIOR SESSION's same-window range, so any
# conditioner whose own observation period contains that session sits on both sides of
# the division. A wide week-so-far implies a wide yesterday implies a big denominator
# implies a small ratio — with no forward-looking content whatsoever. Those
# conditioners get their ratio spread flagged rather than dropped: the raw-range column
# is still theirs to read, and hiding the row would hide the confound too.
RATIO_DENOMINATOR_OVERLAP = [
    "Week_Return_To_Prior_Close_Pct",
    "Week_Range_So_Far_Pct",
    # Overlaps on Mondays only, when the prior session falls in the prior week.
    "Prior_Week_Bull_Bear",
]
# Magnitude first, then sign. `Window_Log_Range_Ratio` is the one that has to move.
WINDOW_MAGNITUDE_TARGETS = ["Window_Range_Pct", "Window_Log_Range_Ratio",
                            "Window_High_Excursion_Pct", "Window_Low_Excursion_Pct"]
WINDOW_SIGNED_TARGETS = ["Window_Return_Pct", "Next_Window_Return_Pct"]
WINDOW_BINARY_TARGETS = ["Window_Bullish"]
# Every column describing the window being scored, or anything after it. Targets,
# never conditioners — the mirror of WHOLE_MONTH_LABELS, guarded by the same style
# of test.
WINDOW_OUTCOME_LABELS = (WINDOW_MAGNITUDE_TARGETS + WINDOW_SIGNED_TARGETS
                         + WINDOW_BINARY_TARGETS
                         + ["Window_Close", "Window_High", "Window_Low", "Window_Range_Ratio"])


def is_window_outcome_label(column: str) -> bool:
    """True when `column` describes the window being scored rather than its past."""
    return column in WINDOW_OUTCOME_LABELS


def bootstrap_mean_ci(values, n_resamples: int = BOOTSTRAP_RESAMPLES, seed: int = 42,
                      clusters=None) -> dict:
    """
    Observed mean and bootstrap 95% CI for a continuous column.

    The probability twin above cannot be reused: these targets are ranges and returns,
    not indicators. `clusters` (normally Week_Start) resamples whole weeks, for the
    same reason it does there — four windows a day across five days is twenty
    correlated rows per week, and resampling rows would report a CI several times
    narrower than the real uncertainty.
    """
    series = pd.Series(values).astype(float)
    arr = series.dropna().to_numpy()
    n = len(arr)
    if n == 0:
        return {"n": 0, "n_clusters": 0, "mean": np.nan, "ci_low": np.nan, "ci_high": np.nan}
    mean = float(arr.mean())
    rng = np.random.default_rng(seed)

    if clusters is None:
        n_clusters = n
        samples = rng.choice(arr, size=(n_resamples, n), replace=True).mean(axis=1)
    else:
        codes = pd.Series(clusters)[series.notna().to_numpy()].astype("category").cat.codes.to_numpy()
        n_clusters = int(codes.max()) + 1 if len(codes) else 0
        if n_clusters == 0:
            return {"n": n, "n_clusters": 0, "mean": round(mean, 6), "ci_low": np.nan, "ci_high": np.nan}
        sums = np.bincount(codes, weights=arr, minlength=n_clusters)
        counts = np.bincount(codes, minlength=n_clusters).astype(float)
        picks = rng.integers(0, n_clusters, size=(n_resamples, n_clusters))
        samples = sums[picks].sum(axis=1) / counts[picks].sum(axis=1)

    lo, hi = np.percentile(samples, [2.5, 97.5])
    return {
        "n": n,
        "n_clusters": n_clusters,
        "mean": round(mean, 6),
        "ci_low": round(float(lo), 6),
        "ci_high": round(float(hi), 6),
    }


def build_week_state_by_session(df_1m: pd.DataFrame) -> pd.DataFrame:
    """
    One row per session carrying week state as of the END OF THE PREVIOUS SESSION.

    Everything here is lagged by construction. `Week_Range_So_Far_Pct` spans the
    week open to the prior session's close and is NaN on a Monday, because a Monday
    has no prior session inside its week — that NaN is the honest value and is
    reported rather than filled.
    """
    session_keys = intraday_trading_day_index(df_1m.index)
    grouped = df_1m.groupby(session_keys)
    closes = grouped["Close"].last()
    highs = grouped["High"].max()
    lows = grouped["Low"].min()
    opens = grouped["Open"].first()

    frame = pd.DataFrame({
        "Trading_Day": pd.DatetimeIndex(closes.index),
        "Session_Close": closes.to_numpy(),
        "Session_High": highs.to_numpy(),
        "Session_Low": lows.to_numpy(),
        "Session_Open": opens.to_numpy(),
    }).sort_values("Trading_Day").reset_index(drop=True)
    frame["Week_Start"] = trading_week_monday_index(pd.DatetimeIndex(frame["Trading_Day"]))
    frame["Session_Index_In_Week"] = frame.groupby("Week_Start").cumcount()

    week_open = frame.groupby("Week_Start")["Session_Open"].transform("first")
    # Cumulative extremes INCLUDING this session, then shifted, so every figure stops
    # at the previous session's close.
    run_high = frame.groupby("Week_Start")["Session_High"].cummax()
    run_low = frame.groupby("Week_Start")["Session_Low"].cummin()
    prior_close = frame.groupby("Week_Start")["Session_Close"].shift(1)
    frame["_Prior_High"] = run_high.groupby(frame["Week_Start"]).shift(1)
    frame["_Prior_Low"] = run_low.groupby(frame["Week_Start"]).shift(1)

    frame["Week_Return_To_Prior_Close_Pct"] = (prior_close / week_open - 1) * 100
    frame["Week_Range_So_Far_Pct"] = (frame["_Prior_High"] - frame["_Prior_Low"]) / week_open * 100
    frame["Above_Weekly_Open_At_Prior_Close"] = np.where(
        prior_close.isna(), np.nan, prior_close > week_open)

    week_close = frame.groupby("Week_Start")["Session_Close"].last()
    week_first_open = frame.groupby("Week_Start")["Session_Open"].first()
    week_dir = np.where(week_close > week_first_open, "Bullish", "Bearish")
    prior_week_dir = pd.Series(week_dir, index=week_close.index).shift(1)
    frame["Prior_Week_Bull_Bear"] = frame["Week_Start"].map(prior_week_dir)
    return frame.drop(columns=["_Prior_High", "_Prior_Low"])


def build_intraday_window_rows(df_1m: pd.DataFrame) -> pd.DataFrame:
    """
    One row per session × intraday window, with the window's outcome and the path
    that preceded it inside the same session.

    Windows are `RELATIVE_WINDOWS`, which tile the session 18:00 → 17:00. Day-prior
    state spans the session open to the window open, so it is empty for the first
    window of the session and is reported as NaN there.
    """
    records = []
    session_keys = intraday_trading_day_index(df_1m.index)
    for trading_day, day in df_1m.groupby(session_keys, sort=True):
        if len(day) < 2:
            continue
        day = day.sort_index()
        idx = day.index
        opens = day["Open"].to_numpy()
        highs = day["High"].to_numpy()
        lows = day["Low"].to_numpy()
        closes = day["Close"].to_numpy()
        day_open = float(opens[0])

        for window_name, (start_hm, end_hm) in RELATIVE_WINDOWS.items():
            start_ts = _window_timestamp(trading_day, start_hm)
            end_ts = _window_timestamp(trading_day, end_hm)
            if end_ts <= start_ts:
                end_ts += pd.Timedelta(days=1)
            start_pos = int(idx.searchsorted(start_ts, side="left"))
            end_pos = int(idx.searchsorted(end_ts, side="left"))
            if end_pos <= start_pos:
                continue
            w_open = float(opens[start_pos])
            if not w_open:
                continue
            w_close = float(closes[end_pos - 1])
            w_high = float(highs[start_pos:end_pos].max())
            w_low = float(lows[start_pos:end_pos].min())

            # Day-prior path: session open through the bar before the window opens.
            if start_pos > 0:
                prior_close = float(closes[start_pos - 1])
                prior_high = float(highs[:start_pos].max())
                prior_low = float(lows[:start_pos].min())
                day_return_to_open = (prior_close / day_open - 1) * 100 if day_open else np.nan
                day_range_to_open = (prior_high - prior_low) / day_open * 100 if day_open else np.nan
            else:
                day_return_to_open = np.nan
                day_range_to_open = np.nan

            records.append({
                "Trading_Day": trading_day,
                "Split": "Train" if trading_day <= TRAIN_END else "OOS",
                "Weekday": trading_weekday(trading_day),
                "Window_Name": window_name,
                "Window_Open": w_open,
                "Window_Close": w_close,
                "Window_High": w_high,
                "Window_Low": w_low,
                "Bars_Total": int(end_pos - start_pos),
                "Window_Return_Pct": (w_close / w_open - 1) * 100,
                "Window_Bullish": bool(w_close > w_open),
                "Window_Range_Pct": (w_high - w_low) / w_open * 100,
                "Window_High_Excursion_Pct": (w_high - w_open) / w_open * 100,
                "Window_Low_Excursion_Pct": (w_open - w_low) / w_open * 100,
                "Day_Return_To_Window_Open_Pct": day_return_to_open,
                "Day_Range_To_Window_Open_Pct": day_range_to_open,
            })

    rows = pd.DataFrame.from_records(records)
    if rows.empty:
        return rows

    window_order = {name: i for i, name in enumerate(RELATIVE_WINDOWS)}
    rows["Window_Order"] = rows["Window_Name"].map(window_order)
    rows = rows.sort_values(["Trading_Day", "Window_Order"]).reset_index(drop=True)

    # The window immediately before this one INSIDE the same session. Shifting within
    # the session, not across it, keeps the overnight gap out of the conditioner.
    by_day = rows.groupby("Trading_Day")
    rows["Prior_Window_Return_Pct"] = by_day["Window_Return_Pct"].shift(1)
    rows["Prior_Window_Range_Pct"] = by_day["Window_Range_Pct"].shift(1)
    # The signed follow-through target: the NEXT window's return, so the row's own
    # window is the conditioner's observation period and the target is strictly after.
    rows["Next_Window_Return_Pct"] = by_day["Window_Return_Pct"].shift(-1)

    # The volatility-clustering denominator: the SAME window one session earlier.
    prior_day = rows.sort_values(["Window_Name", "Trading_Day"]).groupby("Window_Name")
    rows["Prior_Day_Same_Window_Range_Pct"] = prior_day["Window_Range_Pct"].shift(1)
    rows = rows.sort_values(["Trading_Day", "Window_Order"]).reset_index(drop=True)
    denominator = rows["Prior_Day_Same_Window_Range_Pct"].replace(0.0, np.nan)
    rows["Window_Range_Ratio"] = rows["Window_Range_Pct"] / denominator
    # A zero-range window is a data artifact (a halt, or a single bar), not a real
    # contraction to nothing. log(0) would be -inf and would poison every mean it
    # touched, so those rows drop out of the log target rather than dominating it.
    rows["Window_Log_Range_Ratio"] = np.log(
        rows["Window_Range_Ratio"].replace(0.0, np.nan))

    rows["Week_Start"] = trading_week_monday_index(pd.DatetimeIndex(rows["Trading_Day"]))
    return rows


def attach_week_state_to_windows(rows: pd.DataFrame, session_state: pd.DataFrame) -> pd.DataFrame:
    """Join the lagged week-state conditioners onto the session × window rows."""
    keep = ["Trading_Day"] + [c for c in WEEK_STATE_CONDITIONERS]
    state = session_state[keep]
    out = rows.copy()
    out["Trading_Day"] = pd.to_datetime(out["Trading_Day"])
    return out.merge(state, on="Trading_Day", how="left")


def apply_week_context_buckets(rows: pd.DataFrame, features: list = None) -> pd.DataFrame:
    """
    Train-quantile p25/p75 buckets per window × conditioner.

    Cut per window, not pooled: an overnight window's return distribution is much
    tighter than an RTH one, and a pooled cut would sort windows rather than states.
    """
    features = features or WEEK_CONTEXT_NUMERIC_CONDITIONERS
    out = rows.copy()
    for feature in features:
        if feature not in out:
            continue
        bucket_col = f"{feature}_Bucket"
        out[bucket_col] = "Middle 25-75%"
        out.loc[out[feature].isna(), bucket_col] = np.nan
        for window_name, group in out.groupby("Window_Name", dropna=False):
            vals = group[group["Split"].eq("Train")][feature].dropna()
            if vals.empty:
                continue
            q25, q75 = vals.quantile([0.25, 0.75])
            mask = out["Window_Name"].eq(window_name) & out[feature].notna()
            out.loc[mask & (out[feature] <= q25), bucket_col] = "P25 Low"
            out.loc[mask & (out[feature] >= q75), bucket_col] = "P75 High"
    return out


def window_outcomes_by_state(rows: pd.DataFrame, sparse_n: int = SPARSE_N) -> pd.DataFrame:
    """
    Window outcomes per Split × Window × conditioner bucket.

    Continuous targets carry a cluster-bootstrapped mean and CI; `Window_Bullish`
    carries the probability twin. Both cluster on `Week_Start`.
    """
    conditioners = []
    for name in WEEK_STATE_CONDITIONERS + DAY_PRIOR_CONDITIONERS:
        column = f"{name}_Bucket" if name in WEEK_CONTEXT_NUMERIC_CONDITIONERS else name
        if column in rows:
            conditioners.append((name, column))

    records = []
    for (split, window_name), block in rows.groupby(["Split", "Window_Name"], dropna=False):
        for name, column in conditioners:
            for value, group in block.groupby(column, dropna=True):
                clusters = group["Week_Start"]
                record = {
                    "Split": split,
                    "Window_Name": window_name,
                    "Conditioner": name,
                    "Conditioner_Value": value,
                    "n": int(len(group)),
                    "n_weeks": int(group["Week_Start"].nunique()),
                    "is_sparse": bool(len(group) < sparse_n),
                }
                for target in WINDOW_MAGNITUDE_TARGETS + WINDOW_SIGNED_TARGETS:
                    stats = bootstrap_mean_ci(group[target], clusters=clusters)
                    record[f"{target}_mean"] = stats["mean"]
                    record[f"{target}_ci_low"] = stats["ci_low"]
                    record[f"{target}_ci_high"] = stats["ci_high"]
                for target in WINDOW_BINARY_TARGETS:
                    stats = bootstrap_probability_ci(group[target], clusters=clusters)
                    record[f"{target}_pct"] = stats["probability"]
                    record[f"{target}_ci_low"] = stats["ci_low"]
                    record[f"{target}_ci_high"] = stats["ci_high"]
                records.append(record)
    return pd.DataFrame.from_records(records)


def window_conditioner_spreads(summary: pd.DataFrame) -> pd.DataFrame:
    """
    P75-High minus P25-Low per Split × Window × conditioner × target.

    This is the table to read first. A conditioner that only rediscovers volatility
    clustering shows a wide `Window_Range_Pct` spread and a `Window_Log_Range_Ratio`
    spread near zero; only the ratio row is evidence of higher-timeframe information.
    The ratio spread is in logs, so `exp(spread)` is the multiplicative factor between
    the two buckets. Two-bucket conditioners are skipped — the spread is defined on
    the quantile cut.

    `ratio_denominator_overlap` marks the rows where the ratio is not interpretable
    because the conditioner contains the prior session the ratio divides by; see
    RATIO_DENOMINATOR_OVERLAP. Read the raw-range spread for those, never the ratio.
    """
    targets = [f"{t}_mean" for t in WINDOW_MAGNITUDE_TARGETS + WINDOW_SIGNED_TARGETS]
    records = []
    for keys, group in summary.groupby(["Split", "Window_Name", "Conditioner"], dropna=False):
        split, window_name, conditioner = keys
        indexed = group.set_index("Conditioner_Value")
        if not {"P25 Low", "P75 High"} <= set(indexed.index):
            continue
        for target in targets:
            low, high = indexed.loc["P25 Low", target], indexed.loc["P75 High", target]
            if pd.isna(low) or pd.isna(high):
                continue
            # Non-overlapping CIs at the two ends, as a coarse significance read.
            lo_hi = indexed.loc["P25 Low", target.replace("_mean", "_ci_high")]
            hi_lo = indexed.loc["P75 High", target.replace("_mean", "_ci_low")]
            records.append({
                "Split": split,
                "Window_Name": window_name,
                "Conditioner": conditioner,
                "Target": target.replace("_mean", ""),
                "n_low": int(indexed.loc["P25 Low", "n"]),
                "n_high": int(indexed.loc["P75 High", "n"]),
                "low_mean": round(float(low), 6),
                "high_mean": round(float(high), 6),
                "spread": round(float(high) - float(low), 6),
                "ci_disjoint": bool(hi_lo > lo_hi or indexed.loc["P75 High", target.replace("_mean", "_ci_high")] < indexed.loc["P25 Low", target.replace("_mean", "_ci_low")]),
                "ratio_denominator_overlap": bool(
                    target.startswith("Window_Log_Range_Ratio")
                    and conditioner in RATIO_DENOMINATOR_OVERLAP),
            })
    out = pd.DataFrame.from_records(records)
    if out.empty:
        return out
    return out.sort_values(["Split", "Target", "spread"], ascending=[True, True, False]).reset_index(drop=True)


def run_week_context_research(symbol: str = None, path: str = DATA_PATH,
                              output_dir: Path = None) -> tuple:
    """Week-state and day-prior-state conditioning of intraday window paths."""
    symbol = symbol or SYMBOL
    out_dir = output_dir or module_output_dir("week_context", symbol)
    print(f"[Research] {symbol} week-state intraday window context ...")
    df_1m = load_1m_source(path)

    session_state = build_week_state_by_session(df_1m)
    rows = build_intraday_window_rows(df_1m)
    rows = attach_week_state_to_windows(rows, session_state)
    rows = apply_week_context_buckets(rows)
    _write_csv(rows, "intraday_window_rows", output_dir=out_dir)

    summary = window_outcomes_by_state(rows)
    _write_csv(summary, "window_outcomes_by_state", output_dir=out_dir)

    spreads = window_conditioner_spreads(summary)
    _write_csv(spreads, "window_conditioner_spreads", output_dir=out_dir)

    if not spreads.empty:
        train = spreads[spreads["Split"].eq("Train")]
        print("\n[Research] Magnitude: raw range spread vs log-range-ratio spread (Train)")
        print("  ratio spread is in logs; exp(spread) is the factor. Only interpretable where overlap=False")
        pivot = train[train["Target"].isin(["Window_Range_Pct", "Window_Log_Range_Ratio"])].copy()
        pivot["Target"] = np.where(pivot["ratio_denominator_overlap"],
                                   pivot["Target"] + " (overlap)", pivot["Target"])
        print(pivot.pivot_table(index=["Window_Name", "Conditioner"], columns="Target",
                                values="spread").round(4).to_string())
    return rows, summary, spreads


# ═══════════════════════════════════════════════════════════════════════════════
# CHART UTILITIES
# ═══════════════════════════════════════════════════════════════════════════════

def _save(fig: plt.Figure, name: str, output_dir: Path = None):
    base = Path(output_dir) if output_dir is not None else OUTPUT_DIR
    path = base / f"{name}.png"
    path.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(path, dpi=150, bbox_inches="tight")
    print(f"  → {path}")
    plt.close(fig)


def _subtitle(ax: plt.Axes, text: str):
    """Add a small subtitle below the main title."""
    ax.set_title(text, fontsize=10, color="#555555", pad=4)


def _label_bars(ax: plt.Axes, min_pct: float = 0.5):
    """Annotate each bar with its height (%), skip tiny bars."""
    for bar in ax.patches:
        h = bar.get_height()
        if h >= min_pct:
            ax.text(
                bar.get_x() + bar.get_width() / 2,
                h + 0.4,
                f"{h:.1f}%",
                ha="center", va="bottom", fontsize=7.5,
            )


# ═══════════════════════════════════════════════════════════════════════════════
# STANDARD ANALYSES
# ═══════════════════════════════════════════════════════════════════════════════

def chart_day_distribution(weekly: pd.DataFrame, symbol: str = None, resample_to: str = None, output_dir: Path = None):
    """
    Two grouped-bar charts:
      • Which weekday did the weekly LOW form?
      • Which weekday did the weekly HIGH form?
    Each chart splits by Bullish / Bearish weeks.
    """
    symbol, resample_to = symbol or SYMBOL, resample_to or RESAMPLE_TO
    for extreme, col in [("LOW", "Low_Weekday"), ("HIGH", "High_Weekday")]:
        fig, ax = plt.subplots(figsize=(10, 5))
        x = np.arange(len(DAYS))
        w = 0.35

        for i, wt in enumerate(["Bullish", "Bearish"]):
            sub = weekly[weekly["Bull_Bear"] == wt]
            pct = sub[col].value_counts(normalize=True).mul(100).reindex(DAYS, fill_value=0)
            bars = ax.bar(x + (i - 0.5) * w, pct, w, label=wt,
                          color=COLORS[wt], alpha=0.85, edgecolor="white")

        _label_bars(ax)
        ax.set_xticks(x)
        ax.set_xticklabels(DAYS)
        ax.set_ylabel("% of weeks")
        ax.set_title(
            f"{symbol} — Day when weekly {extreme} formed  "
            f"[resampled: {resample_to}]"
        )
        ax.legend()
        ax.yaxis.set_major_formatter(mticker.PercentFormatter())
        ax.grid(axis="y", alpha=0.25, linestyle="--")
        fig.tight_layout()
        _save(fig, f"1_weekly_{extreme.lower()}_day", output_dir=output_dir)


def chart_session_distribution(weekly: pd.DataFrame, symbol: str = None, resample_to: str = None, output_dir: Path = None):
    """
    Two grouped-bar charts: which session did the weekly LOW / HIGH form in?
    """
    symbol, resample_to = symbol or SYMBOL, resample_to or RESAMPLE_TO
    for extreme, col in [("LOW", "Low_Session"), ("HIGH", "High_Session")]:
        fig, ax = plt.subplots(figsize=(10, 5))
        x = np.arange(len(SESSION_ORDER))
        w = 0.35

        for i, wt in enumerate(["Bullish", "Bearish"]):
            sub = weekly[weekly["Bull_Bear"] == wt]
            pct = sub[col].value_counts(normalize=True).mul(100).reindex(SESSION_ORDER, fill_value=0)
            ax.bar(x + (i - 0.5) * w, pct, w, label=wt,
                   color=COLORS[wt], alpha=0.85, edgecolor="white")

        _label_bars(ax)
        ax.set_xticks(x)
        ax.set_xticklabels(SESSION_ORDER)
        ax.set_ylabel("% of weeks")
        ax.set_title(
            f"{symbol} — Session when weekly {extreme} formed  "
            f"[resampled: {resample_to}]"
        )
        ax.legend()
        ax.yaxis.set_major_formatter(mticker.PercentFormatter())
        ax.grid(axis="y", alpha=0.25, linestyle="--")
        fig.tight_layout()
        _save(fig, f"2_weekly_{extreme.lower()}_session", output_dir=output_dir)


def chart_hour_distribution(weekly: pd.DataFrame, symbol: str = None, resample_to: str = None, output_dir: Path = None):
    """
    Four-panel chart: hour-of-day distributions for LOW and HIGH,
    split by Bullish / Bearish.
    """
    symbol, resample_to = symbol or SYMBOL, resample_to or RESAMPLE_TO
    fig, axes = plt.subplots(2, 2, figsize=(14, 8), sharey=False)
    fig.suptitle(
        f"{symbol} — Hour (ET) when weekly extreme formed  [resampled: {resample_to}]",
        fontsize=12,
    )

    combos = [
        (axes[0, 0], "LOW",  "Low_Hour",  "Bullish"),
        (axes[0, 1], "LOW",  "Low_Hour",  "Bearish"),
        (axes[1, 0], "HIGH", "High_Hour", "Bullish"),
        (axes[1, 1], "HIGH", "High_Hour", "Bearish"),
    ]

    for ax, extreme, col, wt in combos:
        sub = weekly[weekly["Bull_Bear"] == wt]
        pct = (
            sub[col].value_counts(normalize=True).mul(100)
            .reindex(range(24), fill_value=0)
        )
        ax.bar(pct.index, pct.values, color=COLORS[wt], alpha=0.85,
               width=0.8, edgecolor="white")
        ax.set_title(f"{extreme} — {wt} weeks")
        ax.set_xlabel("Hour (ET)")
        ax.set_ylabel("% of weeks")
        ax.set_xticks(range(0, 24, 2))
        ax.yaxis.set_major_formatter(mticker.PercentFormatter())
        ax.grid(axis="y", alpha=0.25, linestyle="--")

    fig.tight_layout()
    _save(fig, "3_weekly_extreme_hours", output_dir=output_dir)


def chart_day_session_heatmap(weekly: pd.DataFrame, symbol: str = None, resample_to: str = None, output_dir: Path = None):
    """
    Two heatmaps (Bullish / Bearish) for LOW and HIGH:
    weekday × session joint distribution.
    """
    symbol, resample_to = symbol or SYMBOL, resample_to or RESAMPLE_TO
    for extreme, day_col, sess_col in [
        ("LOW",  "Low_Weekday",  "Low_Session"),
        ("HIGH", "High_Weekday", "High_Session"),
    ]:
        fig, axes = plt.subplots(1, 2, figsize=(13, 5))
        fig.suptitle(
            f"{symbol} — {extreme}: Weekday × Session  [resampled: {resample_to}]",
            fontsize=12,
        )

        for ax, wt in zip(axes, ["Bullish", "Bearish"]):
            sub = weekly[weekly["Bull_Bear"] == wt]
            ct = (
                pd.crosstab(sub[day_col], sub[sess_col], normalize=True)
                .mul(100)
                .reindex(DAYS, fill_value=0)
                .reindex(columns=SESSION_ORDER, fill_value=0)
            )

            im = ax.imshow(ct.values, cmap="YlOrRd", aspect="auto", vmin=0)
            ax.set_xticks(range(len(SESSION_ORDER)))
            ax.set_xticklabels(SESSION_ORDER, rotation=20, ha="right", fontsize=9)
            ax.set_yticks(range(len(DAYS)))
            ax.set_yticklabels(DAYS)
            ax.set_title(f"{wt} weeks  (n={len(sub)})")

            vmax = ct.values.max()
            for r in range(ct.shape[0]):
                for c in range(ct.shape[1]):
                    v = ct.values[r, c]
                    if v > 0.1:
                        color = "white" if v > vmax * 0.6 else "black"
                        ax.text(c, r, f"{v:.1f}%", ha="center", va="center",
                                fontsize=8, color=color)

            plt.colorbar(im, ax=ax, label="% of weeks")

        fig.tight_layout()
        _save(fig, f"4_weekly_{extreme.lower()}_day_session_heatmap", output_dir=output_dir)


# ═══════════════════════════════════════════════════════════════════════════════
# EXPERIMENT FRAMEWORK
# ═══════════════════════════════════════════════════════════════════════════════

def run_experiment(
    weekly: pd.DataFrame,
    factor_col: str,
    target_col: str,
    factor_order: list = None,
    target_order: list = None,
    title: str = None,
    filename: str = None,
    symbol: str = None,
    resample_to: str = None,
    output_dir: Path = None,
):
    """
    Test whether `factor_col` (X) predicts `target_col` (Y).

    Produces one subplot per unique value of X, each showing:
      • Bar chart of P(Y | X=x)  — the conditional distribution
      • Dashed line of P(Y)       — the unconditional baseline

    A factor is "interesting" if the bars deviate significantly from
    the baseline line.

    Args:
        weekly       : DataFrame from build_weekly()
        factor_col   : column to condition on (e.g. "Bull_Bear", "Prev_Bull_Bear")
        target_col   : column to predict (e.g. "Low_Weekday", "High_Session")
        factor_order : ordered list of X values; auto-detected if None
        target_order : ordered list of Y values; auto-detected if None
        title        : chart suptitle; auto-generated if None
        filename     : output filename stem; auto-generated if None

    Examples:
        # Does week direction predict which day the low forms?
        run_experiment(weekly, "Bull_Bear", "Low_Weekday",
                       target_order=DAYS)

        # Does prev-week direction predict this week's direction?
        run_experiment(weekly, "Prev_Bull_Bear", "Bull_Bear")

        # Does which session the low formed in predict which day the high forms?
        run_experiment(weekly, "Low_Session", "High_Weekday",
                       factor_order=SESSION_ORDER, target_order=DAYS)
    """
    symbol, resample_to = symbol or SYMBOL, resample_to or RESAMPLE_TO
    clean = weekly[[factor_col, target_col]].dropna()

    if factor_order is None:
        factor_order = sorted(clean[factor_col].unique())
    if target_order is None:
        target_order = sorted(clean[target_col].unique())

    # P(Y | X)
    ct = (
        pd.crosstab(clean[factor_col], clean[target_col], normalize="index")
        .mul(100)
        .reindex(index=factor_order, columns=target_order, fill_value=0)
    )

    # Baseline P(Y)
    baseline = (
        clean[target_col]
        .value_counts(normalize=True)
        .mul(100)
        .reindex(target_order, fill_value=0)
    )

    n_factors = len(factor_order)
    fig, axes = plt.subplots(
        1, n_factors,
        figsize=(max(5 * n_factors, 8), 5),
        sharey=True,
    )
    if n_factors == 1:
        axes = [axes]

    palette = plt.cm.tab10.colors
    x = np.arange(len(target_order))
    bar_width = 0.6

    for ax, factor_val in zip(axes, factor_order):
        row = ct.loc[factor_val] if factor_val in ct.index else pd.Series(0, index=target_order)
        n = int((clean[factor_col] == factor_val).sum())

        color = COLORS.get(str(factor_val),
                           palette[factor_order.index(factor_val) % len(palette)])
        bars = ax.bar(x, row, bar_width, color=color, alpha=0.85, edgecolor="white",
                      label=str(factor_val))

        # Baseline line
        ax.plot(x, baseline.values, color="black", linestyle="--",
                linewidth=1.4, marker="o", markersize=4,
                label="Baseline (all weeks)", zorder=5)

        # Annotations
        for bar in bars:
            h = bar.get_height()
            if h >= 0.5:
                ax.text(bar.get_x() + bar.get_width() / 2, h + 0.5, f"{h:.1f}%",
                        ha="center", va="bottom", fontsize=7.5)

        ax.set_xticks(x)
        ax.set_xticklabels(target_order, rotation=30, ha="right", fontsize=9)
        ax.set_ylabel("% of weeks")
        ax.set_title(f"{factor_col} = {factor_val}  (n={n})")
        ax.yaxis.set_major_formatter(mticker.PercentFormatter())
        ax.legend(fontsize=8)
        ax.grid(axis="y", alpha=0.25, linestyle="--")

    chart_title = title or f"Experiment: {factor_col}  →  {target_col}"
    fig.suptitle(
        f"{symbol} — {chart_title}  [resampled: {resample_to}]",
        fontsize=12,
    )
    fig.tight_layout()

    stem = filename or f"exp_{factor_col}__{target_col}".replace(" ", "_")
    _save(fig, stem, output_dir=output_dir)


# ═══════════════════════════════════════════════════════════════════════════════
# MAIN
# ═══════════════════════════════════════════════════════════════════════════════

MODULES = [
    "weekly_charts", "weekly_events", "weekly_open_revisit", "intraday_levels",
    "path_dependency", "relative_path", "monthly_extremes", "monthly_levels", "month_context",
    "week_context",
]


def run_weekly_charts(weekly: pd.DataFrame, symbol: str, resample_to: str, output_dir: Path) -> None:
    """Standard weekly distribution charts and the built-in experiment set."""
    print("\n[Charts] Day distributions ...")
    chart_day_distribution(weekly, symbol=symbol, resample_to=resample_to, output_dir=output_dir)
    print("[Charts] Session distributions ...")
    chart_session_distribution(weekly, symbol=symbol, resample_to=resample_to, output_dir=output_dir)
    print("[Charts] Hour distributions ...")
    chart_hour_distribution(weekly, symbol=symbol, resample_to=resample_to, output_dir=output_dir)
    print("[Charts] Day × Session heatmaps ...")
    chart_day_session_heatmap(weekly, symbol=symbol, resample_to=resample_to, output_dir=output_dir)

    print("\n[Experiments]")
    experiments = [
        ("Bull_Bear", "Low_Weekday", ["Bullish", "Bearish"], DAYS, "Does week direction predict LOW weekday?"),
        ("Bull_Bear", "High_Weekday", ["Bullish", "Bearish"], DAYS, "Does week direction predict HIGH weekday?"),
        ("Prev_Bull_Bear", "Bull_Bear", ["Bullish", "Bearish"], ["Bullish", "Bearish"], "Does prev-week direction predict current week direction?"),
        ("Bull_Bear", "Low_Session", ["Bullish", "Bearish"], SESSION_ORDER, "Does week direction predict LOW session?"),
        ("Bull_Bear", "High_Session", ["Bullish", "Bearish"], SESSION_ORDER, "Does week direction predict HIGH session?"),
    ]
    for factor, target, factor_order, target_order, title in experiments:
        run_experiment(
            weekly, factor, target,
            factor_order=factor_order, target_order=target_order, title=title,
            symbol=symbol, resample_to=resample_to, output_dir=output_dir,
        )


def run_research(
    symbol: str = None,
    data_path: str = None,
    resample_to: str = None,
    output_dir: Path = None,
    modules: list = None,
) -> dict:
    """
    Run the selected research modules for one symbol.

    Every module writes to its own symbol-scoped directory, so ES and NQ runs
    never overwrite each other.
    """
    symbol = (symbol or SYMBOL).upper()
    data_path = str(data_path or DATA_PATH)
    resample_to = resample_to or RESAMPLE_TO
    root = Path(output_dir) if output_dir is not None else OUTPUT_DIR
    events_root = root / "research_events"
    modules = list(modules or MODULES)
    unknown = [m for m in modules if m not in MODULES]
    if unknown:
        raise ValueError(f"Unknown module(s): {', '.join(unknown)}. Known: {', '.join(MODULES)}")

    print(f"Loading  {data_path} ...")
    df = load_and_resample(data_path, resample_to)
    print(f"  {len(df):,} bars  ({df.index[0].date()} → {df.index[-1].date()})")

    print("Building weekly summary ...")
    weekly = build_weekly(df)
    bull = (weekly["Bull_Bear"] == "Bullish").sum()
    bear = (weekly["Bull_Bear"] == "Bearish").sum()
    print(f"  {len(weekly)} weeks  |  Bullish: {bull}  |  Bearish: {bear}")

    results = {"weekly": weekly}
    if "weekly_charts" in modules:
        run_weekly_charts(weekly, symbol, resample_to, root / symbol.lower())
    if "weekly_events" in modules:
        results["event_rows"] = run_event_distribution_research(
            df, weekly, symbol=symbol, output_dir=module_output_dir("weekly_events", symbol, events_root))
    if "weekly_open_revisit" in modules:
        results["weekly_open_revisit"] = run_weekly_open_revisit_research(
            data_path, weekly, symbol=symbol, output_dir=module_output_dir("weekly_open_revisit", symbol, events_root))
    if "intraday_levels" in modules:
        results["intraday_levels"] = run_intraday_key_level_research(
            data_path, symbol=symbol, output_dir=module_output_dir("intraday_levels", symbol, events_root))
    if "path_dependency" in modules:
        results["path_dependency"] = run_intraday_to_weekly_path_dependency_research(
            symbol, data_path, output_dir=module_output_dir("path_dependency", symbol, events_root), resample_to=resample_to)
    if "relative_path" in modules:
        results["relative_path"] = run_relative_path_research(
            symbol, data_path, output_dir=module_output_dir("relative_path", symbol, events_root), resample_to=resample_to)
    # monthly_extremes runs off the resampled frame, as the weekly charts do.
    # monthly_levels and month_context need 1-minute bars.
    if "monthly_extremes" in modules:
        results["monthly_extremes"] = run_monthly_extremes_research(
            df, symbol=symbol, output_dir=module_output_dir("monthly_extremes", symbol, events_root))
    if "monthly_levels" in modules:
        results["monthly_levels"] = run_monthly_levels_research(
            symbol, data_path, output_dir=module_output_dir("monthly_levels", symbol, events_root))
    if "month_context" in modules:
        results["month_context"] = run_month_context_research(
            symbol, data_path, output_dir=module_output_dir("month_context", symbol, events_root),
            resample_to=resample_to)
    if "week_context" in modules:
        results["week_context"] = run_week_context_research(
            symbol, data_path, output_dir=module_output_dir("week_context", symbol, events_root))

    print(f"\nDone. Output written under  {root}/")
    return results


def build_arg_parser():
    import argparse

    parser = argparse.ArgumentParser(
        prog="po3_research",
        description="Run PO3 futures path research for one symbol.",
    )
    parser.add_argument("--symbol", default=SYMBOL, help=f"Symbol label for titles and output paths. Default: {SYMBOL}")
    parser.add_argument("--data", default=DATA_PATH, help=f"1-minute OHLCV parquet. Default: {DATA_PATH}")
    parser.add_argument("--resample-to", default=RESAMPLE_TO, help=f"Resample interval for weekly work. Default: {RESAMPLE_TO}")
    parser.add_argument("--output-dir", type=Path, default=OUTPUT_DIR, help=f"Output root. Default: {OUTPUT_DIR}")
    parser.add_argument("--modules", nargs="+", choices=MODULES, default=MODULES, metavar="MODULE",
                        help=f"Modules to run. Choices: {', '.join(MODULES)}. Default: all")
    return parser


def main(argv: list = None):
    args = build_arg_parser().parse_args(argv)
    run_research(
        symbol=args.symbol,
        data_path=args.data,
        resample_to=args.resample_to,
        output_dir=args.output_dir,
        modules=args.modules,
    )


if __name__ == "__main__":
    main()
