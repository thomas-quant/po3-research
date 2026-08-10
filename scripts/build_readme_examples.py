#!/usr/bin/env python3
"""Build tracked README example charts for the PO3 research toolkit."""

from __future__ import annotations

import argparse
import sys
from dataclasses import dataclass
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

import matplotlib

matplotlib.use("Agg")

import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
import numpy as np
import pandas as pd

from po3_research.research import (
    DAYS,
    SESSION_ORDER,
    TRAIN_END,
    build_relative_level_path_rows,
    build_weekly,
    drop_lookahead_level_windows,
    intraday_level_forward_touch_distribution,
    load_1m_source,
    load_and_resample,
    trading_week_monday,
)

DEFAULT_SYMBOL = "ES"
DEFAULT_DATA_PATH = Path("data/es_1m.parquet")
DEFAULT_OUTPUT_DIR = Path("output/examples")
DEFAULT_RESAMPLE_TO = "1h"

BULL = "#2E8B57"
BEAR = "#C44536"
BLUE = "#2F6F9F"
GOLD = "#C99700"
GRID = "#D9DEE7"
TEXT = "#20242A"
MUTED = "#5D6673"

FINDINGS_COLUMNS = ["Symbol", "Metric", "Split", "Segment", "Value", "Context"]
TWAP_VWAP_COLUMNS = [
    "Symbol",
    "Split",
    "Level_Name",
    "Window_Name",
    "Signal",
    "Target",
    "Baseline_Pct",
    "Above_Pct",
    "Below_Pct",
    "Above_Delta_Ppt",
    "Below_Delta_Ppt",
    "End_State_Baseline_Pct",
    "End_State_Adjusted_Delta_Ppt",
    "Abs_Best_Delta_Ppt",
    "Best_Side",
    "Weekday_Scope",
    "n_above",
    "n_below",
    "n_weeks",
]

LEVEL_WINDOW_PAIRS = [
    ("Globex_Open", "Globex_to_Midnight"),
    ("NY_Midnight_Open", "Midnight_to_0930"),
    ("NY_0930_Open", "0930_to_1300"),
    ("NY_1300_Open", "1300_to_Close"),
]

TWAP_VWAP_TARGETS = [
    ("Next session bullish", "Next_Session_Bullish"),
    ("Next session positive return", "Next_Session_Positive_Return"),
    ("Day bullish", "Day_Bullish"),
    ("Week bullish", "Week_Bullish"),
    ("Weekly high Friday", "Weekly_High_Friday"),
    ("Weekly low Monday", "Weekly_Low_Monday"),
]

WEEKLY_TARGET_COLUMNS = {"Week_Bullish", "Weekly_High_Friday", "Weekly_Low_Monday"}
EARLY_WEEK_DAYS = ["Monday", "Tuesday"]


def _is_same_level_close_state(rows: pd.DataFrame, target_col: str) -> bool:
    """
    True when the target is just "did the day close above this level?".

    Checking the target name is not enough: for Globex_Open the level value IS the
    day open, so `Day_Bullish` and `Day_Close_Above_Level` are the same column by
    construction. Comparing the values catches that case for any level.
    """
    if target_col not in rows or "Day_Close_Above_Level" not in rows:
        return False
    target = rows[target_col]
    if target.isna().any():
        return False
    return bool(target.astype(bool).equals(rows["Day_Close_Above_Level"].astype(bool)))


@dataclass(frozen=True)
class GallerySpec:
    filename: str
    title: str


def gallery_specs(symbol: str = DEFAULT_SYMBOL) -> list[GallerySpec]:
    """Stable README gallery file list."""
    s = symbol.lower()
    label = symbol.upper()
    return [
        GallerySpec(f"{s}_weekly_low_day_distribution.png", f"{label} weekly low weekday distribution"),
        GallerySpec(f"{s}_weekly_high_day_distribution.png", f"{label} weekly high weekday distribution"),
        GallerySpec(f"{s}_weekly_extreme_hour_distribution.png", f"{label} weekly extreme hour distribution"),
        GallerySpec(f"{s}_weekly_day_session_heatmap.png", f"{label} weekly day × session heatmap"),
        GallerySpec(f"{s}_midnight_open_forward_touch_15m.png", f"{label} midnight-open forward-touch probability"),
        GallerySpec(f"{s}_relative_path_context_summary.png", f"{label} relative path context summary"),
    ]

def matrix_specs(symbol: str = DEFAULT_SYMBOL) -> list[GallerySpec]:
    """Stable README TWAP/VWAP matrix file list."""
    s = symbol.lower()
    label = symbol.upper()
    return [GallerySpec(f"{s}_twap_vwap_predictive_matrix.png", f"{label} TWAP/VWAP predictive matrix")]


def parse_symbol_data_args(values: list[str] | None) -> list[tuple[str, Path]]:
    """Parse SYMBOL=PATH CLI pairs."""
    pairs: list[tuple[str, Path]] = []
    for value in values or []:
        if "=" not in value:
            raise ValueError(f"Expected SYMBOL=PATH, got {value!r}")
        symbol, path = value.split("=", 1)
        symbol = symbol.strip().upper()
        if not symbol or not path.strip():
            raise ValueError(f"Expected SYMBOL=PATH, got {value!r}")
        pairs.append((symbol, Path(path.strip())))
    return pairs


def _style_axes(ax: plt.Axes) -> None:
    ax.set_facecolor("#FBFCFE")
    ax.grid(axis="y", color=GRID, linewidth=0.8, linestyle="--", alpha=0.9)
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)
    ax.spines["left"].set_color("#BAC3CF")
    ax.spines["bottom"].set_color("#BAC3CF")
    ax.tick_params(colors=TEXT, labelsize=9)


def _finish(fig: plt.Figure, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(path, dpi=170, bbox_inches="tight")
    plt.close(fig)
    print(f"wrote {path}")


def _pct_by_bull_bear(weekly: pd.DataFrame, col: str) -> pd.DataFrame:
    rows = []
    for regime in ["Bullish", "Bearish"]:
        sub = weekly[weekly["Bull_Bear"].eq(regime)]
        pct = sub[col].value_counts(normalize=True).mul(100).reindex(DAYS, fill_value=0)
        for day, value in pct.items():
            rows.append({"Regime": regime, "Day": day, "Pct": float(value), "n": len(sub)})
    return pd.DataFrame(rows)


def _plot_weekday_distribution(weekly: pd.DataFrame, symbol: str, extreme: str, col: str, output: Path) -> None:
    data = _pct_by_bull_bear(weekly, col)
    fig, ax = plt.subplots(figsize=(11.5, 6.2))
    x = np.arange(len(DAYS))
    width = 0.34
    for offset, regime, color in [(-0.18, "Bullish", BULL), (0.18, "Bearish", BEAR)]:
        sub = data[data["Regime"].eq(regime)]
        bars = ax.bar(x + offset, sub["Pct"], width, label=f"{regime} weeks (n={int(sub['n'].iloc[0])})", color=color, edgecolor="white", linewidth=1)
        for bar in bars:
            h = bar.get_height()
            if h >= 1:
                ax.text(bar.get_x() + bar.get_width() / 2, h + 0.7, f"{h:.1f}%", ha="center", va="bottom", fontsize=8, color=TEXT)
    _style_axes(ax)
    ax.set_xticks(x)
    ax.set_xticklabels(DAYS)
    ax.set_ylabel("Share of weeks")
    ax.yaxis.set_major_formatter(mticker.PercentFormatter())
    ax.set_title(f"{symbol.upper()} weekly {extreme.lower()} formation by weekday", loc="left", fontsize=15, fontweight="bold", color=TEXT)
    ax.text(0, 1.02, "Bullish/bearish split shows whether weekly direction changes the timing profile.", transform=ax.transAxes, color=MUTED, fontsize=10)
    ax.legend(frameon=False, loc="upper right")
    fig.tight_layout()
    _finish(fig, output)


def _plot_hour_distribution(weekly: pd.DataFrame, symbol: str, output: Path) -> None:
    fig, axes = plt.subplots(2, 2, figsize=(13, 8.5), sharex=True)
    fig.suptitle(f"{symbol.upper()} hour-of-day when weekly extremes formed", x=0.06, y=0.99, ha="left", fontsize=15, fontweight="bold", color=TEXT)
    fig.text(0.06, 0.955, "Hours are New York time; each panel is normalized within its weekly direction bucket.", color=MUTED, fontsize=10)
    combos = [
        ("Weekly LOW", "Low_Hour", "Bullish", BULL),
        ("Weekly LOW", "Low_Hour", "Bearish", BEAR),
        ("Weekly HIGH", "High_Hour", "Bullish", BULL),
        ("Weekly HIGH", "High_Hour", "Bearish", BEAR),
    ]
    for ax, (label, col, regime, color) in zip(axes.ravel(), combos):
        sub = weekly[weekly["Bull_Bear"].eq(regime)]
        pct = sub[col].value_counts(normalize=True).mul(100).reindex(range(24), fill_value=0)
        ax.bar(pct.index, pct.values, color=color, width=0.82, edgecolor="white")
        _style_axes(ax)
        ax.set_title(f"{label} — {regime} (n={len(sub)})", loc="left", fontsize=11, color=TEXT)
        ax.set_ylabel("Share")
        ax.set_xticks(range(0, 24, 2))
        ax.yaxis.set_major_formatter(mticker.PercentFormatter())
    fig.supxlabel("Hour (ET)", y=0.04, fontsize=10)
    fig.tight_layout(rect=(0, 0.04, 1, 0.94))
    _finish(fig, output)


def _plot_day_session_heatmap(weekly: pd.DataFrame, symbol: str, output: Path) -> None:
    fig, axes = plt.subplots(2, 2, figsize=(13.5, 9.5), sharex=True, sharey=True)
    fig.suptitle(f"{symbol.upper()} weekly extreme timing: weekday × session", x=0.06, y=0.99, ha="left", fontsize=15, fontweight="bold", color=TEXT)
    fig.text(0.06, 0.958, "Cell values are % of weeks inside each bullish/bearish subset.", color=MUTED, fontsize=10)
    panels = [
        ("LOW — Bullish", "Low_Weekday", "Low_Session", "Bullish"),
        ("LOW — Bearish", "Low_Weekday", "Low_Session", "Bearish"),
        ("HIGH — Bullish", "High_Weekday", "High_Session", "Bullish"),
        ("HIGH — Bearish", "High_Weekday", "High_Session", "Bearish"),
    ]
    vmax = 0.0
    tables = []
    for _, day_col, sess_col, regime in panels:
        sub = weekly[weekly["Bull_Bear"].eq(regime)]
        ct = pd.crosstab(sub[day_col], sub[sess_col], normalize=True).mul(100).reindex(DAYS, fill_value=0).reindex(columns=SESSION_ORDER, fill_value=0)
        tables.append((ct, len(sub)))
        vmax = max(vmax, float(ct.values.max()))
    for ax, panel, table in zip(axes.ravel(), panels, tables):
        title, _, _, _ = panel
        ct, n = table
        im = ax.imshow(ct.values, cmap="YlGnBu", aspect="auto", vmin=0, vmax=vmax)
        ax.set_title(f"{title} (n={n})", loc="left", fontsize=11, color=TEXT)
        ax.set_xticks(range(len(SESSION_ORDER)))
        ax.set_xticklabels(SESSION_ORDER, rotation=25, ha="right")
        ax.set_yticks(range(len(DAYS)))
        ax.set_yticklabels(DAYS)
        for r in range(ct.shape[0]):
            for c in range(ct.shape[1]):
                v = ct.values[r, c]
                if v >= 0.5:
                    ax.text(c, r, f"{v:.1f}", ha="center", va="center", fontsize=8, color="white" if v > vmax * 0.58 else TEXT)
    fig.colorbar(im, ax=axes.ravel().tolist(), shrink=0.88, label="% of weeks")
    fig.tight_layout(rect=(0, 0, 0.94, 0.94))
    _finish(fig, output)


def _plot_forward_touch(df_1m: pd.DataFrame, symbol: str, output: Path) -> None:
    forward = intraday_level_forward_touch_distribution(df_1m, bucket_freq="15min")
    sub = forward[(forward["Split"].eq("Train")) & (forward["Level_Name"].eq("NY_Midnight_Open"))].copy()
    fig, ax = plt.subplots(figsize=(12.5, 6.3))
    x = np.arange(len(sub))
    ax.plot(x, sub["touch_pct"], color=BLUE, linewidth=2.1, marker="o", markersize=3.5)
    ax.fill_between(x, sub["touch_pct"].astype(float).to_numpy(), color=BLUE, alpha=0.12)
    _style_axes(ax)
    step = max(1, len(sub) // 14)
    ax.set_xticks(x[::step])
    ax.set_xticklabels(sub["Bucket_Time"].iloc[::step], rotation=45, ha="right")
    ax.set_ylabel("Probability of later touch")
    ax.yaxis.set_major_formatter(mticker.PercentFormatter())
    ax.set_title(f"{symbol.upper()} NY midnight open: forward-touch probability", loc="left", fontsize=15, fontweight="bold", color=TEXT)
    ax.text(0, 1.02, "Train split only. Each bucket asks: from this time through same-day close, was midnight open touched again?", transform=ax.transAxes, color=MUTED, fontsize=10)
    fig.tight_layout()
    _finish(fig, output)


def _plot_relative_path(df_1m: pd.DataFrame, symbol: str, output: Path) -> None:
    rows = drop_lookahead_level_windows(build_relative_level_path_rows(df_1m))
    focus = rows[(rows["Split"].eq("Train")) & (rows["Window_Name"].eq("Midnight_to_0930"))].copy()
    summary = focus.groupby("Level_Name", as_index=False)[["Pct_Bars_Above_Level", "Pct_Bars_Below_Level", "Pct_Bars_Touching_Level"]].mean()
    summary = summary.set_index("Level_Name").reindex(["Globex_Open", "NY_Midnight_Open", "NY_0930_Open", "NY_1300_Open"]).dropna(how="all")
    fig, ax = plt.subplots(figsize=(12.5, 6.6))
    x = np.arange(len(summary))
    width = 0.26
    cols = ["Pct_Bars_Above_Level", "Pct_Bars_Below_Level", "Pct_Bars_Touching_Level"]
    labels = ["Above level", "Below level", "Touching level"]
    colors = [BULL, BEAR, GOLD]
    for i, (col, label, color) in enumerate(zip(cols, labels, colors)):
        ax.bar(x + (i - 1) * width, summary[col], width, label=label, color=color, edgecolor="white")
    _style_axes(ax)
    ax.set_xticks(x)
    ax.set_xticklabels([name.replace("_", "\n") for name in summary.index])
    ax.set_ylabel("Average % of bars")
    ax.yaxis.set_major_formatter(mticker.PercentFormatter())
    ax.set_title(f"{symbol.upper()} relative path context before 09:30", loc="left", fontsize=15, fontweight="bold", color=TEXT)
    ax.text(0, 1.02, "Train split, Midnight→09:30 window. Only levels already defined by 09:30 are shown.", transform=ax.transAxes, color=MUTED, fontsize=10)
    ax.legend(frameon=False, ncol=3, loc="upper right")
    fig.tight_layout()
    _finish(fig, output)


def _pct(series: pd.Series) -> float:
    clean = series.dropna()
    return round(float(clean.mean() * 100), 4) if len(clean) else np.nan


def _top_category(series: pd.Series) -> tuple[str, float]:
    pct = series.value_counts(normalize=True).mul(100)
    if pct.empty:
        return "n/a", np.nan
    return str(pct.index[0]), round(float(pct.iloc[0]), 2)


def build_globex_midnight_findings(symbol: str, relative_rows: pd.DataFrame) -> list[dict]:
    """
    Reproduce the Globex→Midnight state table.

    Signal is the midnight open against the Globex open. It is scored against two
    targets: the full-day direction (which is measured from the Globex open, so the
    signal partly describes it) and the strictly forward midnight→close leg.
    """
    levels = relative_rows[relative_rows["Window_Name"].eq("Globex_to_Midnight")]
    globex = levels[levels["Level_Name"].eq("Globex_Open")].set_index("Trading_Day")
    midnight = levels[levels["Level_Name"].eq("NY_Midnight_Open")].set_index("Trading_Day")
    shared = globex.index.intersection(midnight.index)
    if shared.empty:
        return []

    frame = pd.DataFrame({
        "Split": globex.loc[shared, "Split"],
        "Midnight_Above_Globex": midnight.loc[shared, "Level_Value"] > globex.loc[shared, "Level_Value"],
        "Day_Bullish": globex.loc[shared, "Day_Bullish"].astype(bool),
        "Midnight_To_Close_Positive": midnight.loc[shared, "Day_Close_Above_Level"].astype(bool),
    })

    rows: list[dict] = []
    for split, group in frame.groupby("Split", dropna=False):
        above = group["Midnight_Above_Globex"].astype(bool)
        for target_col, label in [("Day_Bullish", "Full day bullish"), ("Midnight_To_Close_Positive", "Midnight to close positive")]:
            for segment, subset in [("baseline", group[target_col]), ("midnight above globex", group[target_col][above]), ("midnight at or below globex", group[target_col][~above])]:
                rows.append({
                    "Symbol": symbol.upper(),
                    "Metric": f"Globex to midnight state — {label}",
                    "Split": split,
                    "Segment": segment,
                    "Value": _pct(subset),
                    "Context": f"% of days (n={len(subset)})",
                })
    return rows


def _build_findings_summary(symbol: str, weekly: pd.DataFrame, df_1m: pd.DataFrame, twap_summary: pd.DataFrame, relative_rows: pd.DataFrame = None) -> pd.DataFrame:
    rows: list[dict[str, object]] = []
    symbol = symbol.upper()
    for split, group in [("All", weekly), ("Train", weekly[weekly.index <= TRAIN_END]), ("OOS", weekly[weekly.index > TRAIN_END])]:
        if group.empty:
            continue
        low_day, low_pct = _top_category(group["Low_Weekday"])
        high_day, high_pct = _top_category(group["High_Weekday"])
        bull_pct = round(float(group["Bull_Bear"].eq("Bullish").mean() * 100), 2)
        rows.extend([
            {"Symbol": symbol, "Metric": "Most common weekly low day", "Split": split, "Segment": low_day, "Value": low_pct, "Context": "% of weeks"},
            {"Symbol": symbol, "Metric": "Most common weekly high day", "Split": split, "Segment": high_day, "Value": high_pct, "Context": "% of weeks"},
            {"Symbol": symbol, "Metric": "Bullish week rate", "Split": split, "Segment": "Bullish", "Value": bull_pct, "Context": "% of weeks"},
        ])

    forward = intraday_level_forward_touch_distribution(df_1m, bucket_freq="15min")
    for bucket in ["09:30", "13:00"]:
        sub = forward[(forward["Split"].eq("Train")) & (forward["Level_Name"].eq("NY_Midnight_Open")) & (forward["Bucket_Time"].eq(bucket))]
        if not sub.empty:
            rows.append({"Symbol": symbol, "Metric": "Midnight open later touch", "Split": "Train", "Segment": f"from {bucket}", "Value": round(float(sub["touch_pct"].iloc[0]), 2), "Context": "% of days touched later"})

    if not twap_summary.empty:
        for split in ["Train", "OOS"]:
            sub = twap_summary[twap_summary["Split"].eq(split)].copy()
            sub = sub[sub["End_State_Adjusted_Delta_Ppt"].notna()]
            if sub.empty:
                continue
            best = sub.sort_values("End_State_Adjusted_Delta_Ppt", ascending=False).iloc[0]
            rows.append({
                "Symbol": symbol,
                "Metric": "Strongest non-tautological TWAP/VWAP delta",
                "Split": split,
                "Segment": f"{best['Level_Name']} / {best['Window_Name']} / {best['Signal']} / {best['Target']} / {best['Best_Side']}",
                "Value": round(float(best["End_State_Adjusted_Delta_Ppt"]), 2),
                "Context": "ppt edge remaining after end-close-above-level sanity baseline",
            })

    if relative_rows is not None and not relative_rows.empty:
        rows.extend(build_globex_midnight_findings(symbol, relative_rows))
    return pd.DataFrame(rows, columns=FINDINGS_COLUMNS)


def _weekday_scope(target_col: str) -> str:
    """
    Which rows may condition a target.

    Weekly labels are only forward-looking early in the week: a Friday row asking
    "did the weekly high form on Friday?" is describing its own session, not
    predicting it. Weekly targets are therefore restricted to Monday/Tuesday
    rows, matching relative_path_to_weekly_outcomes.
    """
    return "Mon-Tue" if target_col in WEEKLY_TARGET_COLUMNS else "All"


def _build_twap_vwap_summary(symbol: str, rows: pd.DataFrame, weekly: pd.DataFrame) -> pd.DataFrame:
    detail = rows.copy()
    detail["Week_Start"] = pd.to_datetime(detail["Trading_Day"]).map(trading_week_monday)
    weekly_targets = weekly[["Bull_Bear", "High_Weekday", "Low_Weekday"]].copy()
    detail = detail.join(weekly_targets, on="Week_Start")
    detail["Week_Bullish"] = detail["Bull_Bear"].eq("Bullish")
    detail["Weekly_High_Friday"] = detail["High_Weekday"].eq("Friday")
    detail["Weekly_Low_Monday"] = detail["Low_Weekday"].eq("Monday")
    # Keep NaN as NaN: `> 0` would score every window with no following session
    # (all of 1300_to_Close) as a negative observation instead of no observation.
    detail["Next_Session_Positive_Return"] = np.where(
        detail["Next_Session_Return_Pct"].isna(), np.nan, detail["Next_Session_Return_Pct"] > 0
    )

    targets = TWAP_VWAP_TARGETS
    records: list[dict[str, object]] = []
    for level_name, window_name in LEVEL_WINDOW_PAIRS:
        pair_rows = detail[detail["Level_Name"].eq(level_name) & detail["Window_Name"].eq(window_name)]
        if pair_rows.empty:
            continue
        for split, all_split_rows in pair_rows.groupby("Split", dropna=False):
            for signal in ["TWAP_Above_Level", "VWAP_Above_Level"]:
                if signal not in all_split_rows:
                    continue
                for target_label, target_col in targets:
                    scope = _weekday_scope(target_col)
                    split_rows = all_split_rows[all_split_rows["Weekday"].isin(EARLY_WEEK_DAYS)] if scope == "Mon-Tue" else all_split_rows
                    # A signal row with no VWAP (zero-volume window) cannot vote.
                    split_rows = split_rows[split_rows[signal].notna()]
                    if split_rows.empty:
                        continue
                    if _is_same_level_close_state(split_rows, target_col):
                        continue
                    target = split_rows[target_col]
                    # A window with nothing after it (1300_to_Close) has no
                    # next-session outcome at all. Emit nothing rather than a row
                    # of NaN or a baseline of 0%.
                    if target.notna().sum() == 0:
                        continue
                    above_mask = split_rows[signal].astype(bool)
                    below_mask = ~above_mask
                    baseline = _pct(target)
                    above = _pct(target[above_mask])
                    below = _pct(target[below_mask])
                    above_delta = round(float(above - baseline), 4) if not pd.isna(above) and not pd.isna(baseline) else np.nan
                    below_delta = round(float(below - baseline), 4) if not pd.isna(below) and not pd.isna(baseline) else np.nan
                    if pd.isna(above_delta) and pd.isna(below_delta):
                        best_side = "n/a"
                        abs_best = np.nan
                    elif pd.isna(below_delta) or abs(above_delta) >= abs(below_delta):
                        best_side = "Above"
                        abs_best = abs(above_delta)
                    else:
                        best_side = "Below"
                        abs_best = abs(below_delta)

                    end_mask = split_rows["Window_Close_Above_Level"].astype(bool)
                    end_above = _pct(target[end_mask])
                    end_below = _pct(target[~end_mask])
                    end_above_delta = abs(end_above - baseline) if not pd.isna(end_above) and not pd.isna(baseline) else np.nan
                    end_below_delta = abs(end_below - baseline) if not pd.isna(end_below) and not pd.isna(baseline) else np.nan
                    end_best = np.nanmax([end_above_delta, end_below_delta]) if not (pd.isna(end_above_delta) and pd.isna(end_below_delta)) else np.nan
                    adjusted = abs_best - end_best if not pd.isna(abs_best) and not pd.isna(end_best) else np.nan
                    records.append({
                        "Symbol": symbol.upper(),
                        "Split": split,
                        "Level_Name": level_name,
                        "Window_Name": window_name,
                        "Signal": signal.replace("_Above_Level", ""),
                        "Target": target_label,
                        "Weekday_Scope": scope,
                        "n_weeks": int(split_rows["Week_Start"].nunique()),
                        "Baseline_Pct": baseline,
                        "Above_Pct": above,
                        "Below_Pct": below,
                        "Above_Delta_Ppt": above_delta,
                        "Below_Delta_Ppt": below_delta,
                        "End_State_Baseline_Pct": round(float(end_best), 4) if not pd.isna(end_best) else np.nan,
                        "End_State_Adjusted_Delta_Ppt": round(float(adjusted), 4) if not pd.isna(adjusted) else np.nan,
                        "Abs_Best_Delta_Ppt": round(float(abs_best), 4) if not pd.isna(abs_best) else np.nan,
                        "Best_Side": best_side,
                        "n_above": int(above_mask.sum()),
                        "n_below": int(below_mask.sum()),
                    })
    return pd.DataFrame(records, columns=TWAP_VWAP_COLUMNS)


def _plot_twap_vwap_matrix(summary: pd.DataFrame, symbol: str, output: Path) -> None:
    plot = summary[summary["Split"].eq("OOS")].copy()
    split_label = "OOS"
    if plot.empty:
        plot = summary[summary["Split"].eq("Train")].copy()
        split_label = "Train"
    plot["Row"] = plot["Level_Name"].str.replace("_", " ") + "\n" + plot["Window_Name"].str.replace("_", "→") + "\n" + plot["Signal"]
    plot["Positive_Adjusted_Delta_Ppt"] = plot["End_State_Adjusted_Delta_Ppt"].clip(lower=0)
    pivot = plot.pivot_table(index="Row", columns="Target", values="Positive_Adjusted_Delta_Ppt", aggfunc="max").fillna(0)
    target_order = ["Next session bullish", "Next session positive return", "Day bullish", "Week bullish", "Weekly high Friday", "Weekly low Monday"]
    pivot = pivot.reindex(columns=target_order, fill_value=0)

    fig, ax = plt.subplots(figsize=(13.5, max(6, len(pivot) * 0.58)))
    vmax = max(5.0, float(pivot.to_numpy().max()) if len(pivot) else 5.0)
    im = ax.imshow(pivot.values, cmap="YlOrRd", aspect="auto", vmin=0, vmax=vmax)
    ax.set_title(f"{symbol.upper()} TWAP/VWAP conditional deltas by level window", loc="left", fontsize=15, fontweight="bold", color=TEXT)
    ax.text(0, 1.02, f"{split_label} split. Cell = positive incremental ppt after end-close-above-level sanity baseline. Weekly targets use Mon/Tue rows only.", transform=ax.transAxes, color=MUTED, fontsize=10)
    ax.set_xticks(range(len(pivot.columns)))
    ax.set_xticklabels(pivot.columns, rotation=25, ha="right")
    ax.set_yticks(range(len(pivot.index)))
    ax.set_yticklabels(pivot.index)
    for r in range(pivot.shape[0]):
        for c in range(pivot.shape[1]):
            v = float(pivot.values[r, c])
            ax.text(c, r, f"{v:.1f}", ha="center", va="center", fontsize=8, color="white" if v > vmax * 0.58 else TEXT)
    fig.colorbar(im, ax=ax, label="incremental delta after sanity baseline (ppt)")
    fig.tight_layout()
    _finish(fig, output)


def build_examples(symbol: str, data_path: Path, output_dir: Path, resample_to: str) -> tuple[pd.DataFrame, pd.DataFrame]:
    symbol = symbol.upper()
    specs = {spec.filename: spec for spec in gallery_specs(symbol)}
    matrix = {spec.filename: spec for spec in matrix_specs(symbol)}
    df_1m = load_1m_source(str(data_path))
    df = load_and_resample(str(data_path), resample_to)
    weekly = build_weekly(df)

    _plot_weekday_distribution(weekly, symbol, "LOW", "Low_Weekday", output_dir / specs[f"{symbol.lower()}_weekly_low_day_distribution.png"].filename)
    _plot_weekday_distribution(weekly, symbol, "HIGH", "High_Weekday", output_dir / specs[f"{symbol.lower()}_weekly_high_day_distribution.png"].filename)
    _plot_hour_distribution(weekly, symbol, output_dir / specs[f"{symbol.lower()}_weekly_extreme_hour_distribution.png"].filename)
    _plot_day_session_heatmap(weekly, symbol, output_dir / specs[f"{symbol.lower()}_weekly_day_session_heatmap.png"].filename)
    _plot_forward_touch(df_1m, symbol, output_dir / specs[f"{symbol.lower()}_midnight_open_forward_touch_15m.png"].filename)
    _plot_relative_path(df_1m, symbol, output_dir / specs[f"{symbol.lower()}_relative_path_context_summary.png"].filename)

    relative_rows = build_relative_level_path_rows(df_1m)
    twap_summary = _build_twap_vwap_summary(symbol, relative_rows, weekly)
    _plot_twap_vwap_matrix(twap_summary, symbol, output_dir / matrix[f"{symbol.lower()}_twap_vwap_predictive_matrix.png"].filename)
    findings = _build_findings_summary(symbol, weekly, df_1m, twap_summary, relative_rows)
    return findings, twap_summary


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build README example PNGs from local futures parquet data.")
    parser.add_argument("--symbol", default=DEFAULT_SYMBOL, help="Symbol label for chart titles and filenames. Default: ES")
    parser.add_argument("--data", type=Path, default=DEFAULT_DATA_PATH, help="Path to 1-minute parquet data. Default: data/es_1m.parquet")
    parser.add_argument("--symbol-data", action="append", default=[], metavar="SYMBOL=PATH", help="Add a symbol/data pair. Repeat for ES and NQ. Overrides --symbol/--data when supplied.")
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR, help="Directory for README artifacts. Default: output/examples")
    parser.add_argument("--resample-to", default=DEFAULT_RESAMPLE_TO, help="Resample interval for weekly charts. Default: 1h")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    pairs = parse_symbol_data_args(args.symbol_data) if args.symbol_data else [(args.symbol.upper(), args.data)]
    findings_parts = []
    twap_parts = []
    for symbol, data_path in pairs:
        findings, twap_summary = build_examples(symbol, data_path, args.output_dir, args.resample_to)
        findings_parts.append(findings)
        twap_parts.append(twap_summary)
    if findings_parts:
        pd.concat(findings_parts, ignore_index=True).to_csv(args.output_dir / "readme_findings_summary.csv", index=False)
        print(f"wrote {args.output_dir / 'readme_findings_summary.csv'}")
    if twap_parts:
        pd.concat(twap_parts, ignore_index=True).to_csv(args.output_dir / "readme_twap_vwap_predictive_summary.csv", index=False)
        print(f"wrote {args.output_dir / 'readme_twap_vwap_predictive_summary.csv'}")


if __name__ == "__main__":
    main()
