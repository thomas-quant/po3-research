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

# ═══════════════════════════════════════════════════════════════════════════════
# DATA LOADING & RESAMPLING
# ═══════════════════════════════════════════════════════════════════════════════

def load_and_resample(path: str, resample_to: str) -> pd.DataFrame:
    """
    Load 1-minute parquet, set DateTime_ET as index (localised to US/Eastern),
    then resample OHLCV to `resample_to` frequency.
    """
    df = pd.read_parquet(path)

    # Build a proper tz-aware DatetimeIndex from the ET column
    dt = pd.to_datetime(df["DateTime_ET"])
    dt = dt.dt.tz_localize("America/New_York", ambiguous="infer", nonexistent="shift_forward")
    df.index = dt
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


def trading_weekday(ts: pd.Timestamp) -> str:
    """
    Return the trading weekday name.
    Sunday 18:00+ is the CME open for the new week — treat it as Monday.
    """
    if ts.dayofweek == 6:   # Sunday evening session
        return "Monday"
    return ts.day_name()


def trading_week_monday(ts: pd.Timestamp) -> pd.Timestamp:
    """
    Return the Monday (midnight) of the trading week that `ts` belongs to.

    ES futures reopen Sunday 18:00 ET — that session is the first bar of the
    new Mon–Fri week, so Sunday 18:00+ maps to the NEXT Monday.

    W-MON grouper is WRONG for this: it creates Tue→Mon buckets, so Monday
    always ends up as the last day of the group, artificially inflating it as
    the weekly high/low day.
    """
    dow = ts.dayofweek      # Mon=0 … Fri=4, Sat=5, Sun=6
    if dow == 6:            # Sunday evening (all Sunday data is 18:00–23:59)
        return (ts + pd.Timedelta(days=1)).normalize()
    return (ts - pd.Timedelta(days=dow)).normalize()


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
    week_keys = df.index.map(trading_week_monday)

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
# CHART UTILITIES
# ═══════════════════════════════════════════════════════════════════════════════

def _save(fig: plt.Figure, name: str):
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    path = OUTPUT_DIR / f"{name}.png"
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

def chart_day_distribution(weekly: pd.DataFrame):
    """
    Two grouped-bar charts:
      • Which weekday did the weekly LOW form?
      • Which weekday did the weekly HIGH form?
    Each chart splits by Bullish / Bearish weeks.
    """
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
            f"{SYMBOL} — Day when weekly {extreme} formed  "
            f"[resampled: {RESAMPLE_TO}]"
        )
        ax.legend()
        ax.yaxis.set_major_formatter(mticker.PercentFormatter())
        ax.grid(axis="y", alpha=0.25, linestyle="--")
        fig.tight_layout()
        _save(fig, f"1_weekly_{extreme.lower()}_day")


def chart_session_distribution(weekly: pd.DataFrame):
    """
    Two grouped-bar charts: which session did the weekly LOW / HIGH form in?
    """
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
            f"{SYMBOL} — Session when weekly {extreme} formed  "
            f"[resampled: {RESAMPLE_TO}]"
        )
        ax.legend()
        ax.yaxis.set_major_formatter(mticker.PercentFormatter())
        ax.grid(axis="y", alpha=0.25, linestyle="--")
        fig.tight_layout()
        _save(fig, f"2_weekly_{extreme.lower()}_session")


def chart_hour_distribution(weekly: pd.DataFrame):
    """
    Four-panel chart: hour-of-day distributions for LOW and HIGH,
    split by Bullish / Bearish.
    """
    fig, axes = plt.subplots(2, 2, figsize=(14, 8), sharey=False)
    fig.suptitle(
        f"{SYMBOL} — Hour (ET) when weekly extreme formed  [resampled: {RESAMPLE_TO}]",
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
    _save(fig, "3_weekly_extreme_hours")


def chart_day_session_heatmap(weekly: pd.DataFrame):
    """
    Two heatmaps (Bullish / Bearish) for LOW and HIGH:
    weekday × session joint distribution.
    """
    for extreme, day_col, sess_col in [
        ("LOW",  "Low_Weekday",  "Low_Session"),
        ("HIGH", "High_Weekday", "High_Session"),
    ]:
        fig, axes = plt.subplots(1, 2, figsize=(13, 5))
        fig.suptitle(
            f"{SYMBOL} — {extreme}: Weekday × Session  [resampled: {RESAMPLE_TO}]",
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
        _save(fig, f"4_weekly_{extreme.lower()}_day_session_heatmap")


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
        f"{SYMBOL} — {chart_title}  [resampled: {RESAMPLE_TO}]",
        fontsize=12,
    )
    fig.tight_layout()

    stem = filename or f"exp_{factor_col}__{target_col}".replace(" ", "_")
    _save(fig, stem)


# ═══════════════════════════════════════════════════════════════════════════════
# MAIN
# ═══════════════════════════════════════════════════════════════════════════════

if __name__ == "__main__":
    print(f"Loading  {DATA_PATH} ...")
    df = load_and_resample(DATA_PATH, RESAMPLE_TO)
    print(f"  {len(df):,} bars  ({df.index[0].date()} → {df.index[-1].date()})")

    print("Building weekly summary ...")
    weekly = build_weekly(df)
    bull = (weekly["Bull_Bear"] == "Bullish").sum()
    bear = (weekly["Bull_Bear"] == "Bearish").sum()
    print(f"  {len(weekly)} weeks  |  Bullish: {bull}  |  Bearish: {bear}")

    # ── Standard Distribution Charts ─────────────────────────────────────────
    print("\n[Charts] Day distributions ...")
    chart_day_distribution(weekly)

    print("[Charts] Session distributions ...")
    chart_session_distribution(weekly)

    print("[Charts] Hour distributions ...")
    chart_hour_distribution(weekly)

    print("[Charts] Day × Session heatmaps ...")
    chart_day_session_heatmap(weekly)

    # ── Experiments ──────────────────────────────────────────────────────────
    print("\n[Experiments]")

    # Does week direction predict which day the LOW forms?
    run_experiment(
        weekly, "Bull_Bear", "Low_Weekday",
        factor_order=["Bullish", "Bearish"],
        target_order=DAYS,
        title="Does week direction predict LOW weekday?",
    )

    # Does week direction predict which day the HIGH forms?
    run_experiment(
        weekly, "Bull_Bear", "High_Weekday",
        factor_order=["Bullish", "Bearish"],
        target_order=DAYS,
        title="Does week direction predict HIGH weekday?",
    )

    # Does previous week direction predict current week direction?
    run_experiment(
        weekly, "Prev_Bull_Bear", "Bull_Bear",
        factor_order=["Bullish", "Bearish"],
        target_order=["Bullish", "Bearish"],
        title="Does prev-week direction predict current week direction?",
    )

    # Does week direction predict which session the LOW forms in?
    run_experiment(
        weekly, "Bull_Bear", "Low_Session",
        factor_order=["Bullish", "Bearish"],
        target_order=SESSION_ORDER,
        title="Does week direction predict LOW session?",
    )

    # Does week direction predict which session the HIGH forms in?
    run_experiment(
        weekly, "Bull_Bear", "High_Session",
        factor_order=["Bullish", "Bearish"],
        target_order=SESSION_ORDER,
        title="Does week direction predict HIGH session?",
    )

    # ── Add your own experiments below ───────────────────────────────────────
    # run_experiment(weekly, "Low_Session", "High_Weekday",
    #                factor_order=SESSION_ORDER, target_order=DAYS)
    # run_experiment(weekly, "Low_Weekday", "High_Weekday",
    #                factor_order=DAYS, target_order=DAYS)

    print(f"\nDone. Charts saved to  {OUTPUT_DIR}/")
