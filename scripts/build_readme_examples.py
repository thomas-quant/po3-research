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
    build_relative_level_path_rows,
    build_weekly,
    intraday_level_forward_touch_distribution,
    load_1m_source,
    load_and_resample,
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
    rows = build_relative_level_path_rows(df_1m)
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
    ax.text(0, 1.02, "Train split, Midnight→09:30 window. Bars are classified relative to each defined key open.", transform=ax.transAxes, color=MUTED, fontsize=10)
    ax.legend(frameon=False, ncol=3, loc="upper right")
    fig.tight_layout()
    _finish(fig, output)


def build_examples(symbol: str, data_path: Path, output_dir: Path, resample_to: str) -> None:
    symbol = symbol.upper()
    specs = {spec.filename: spec for spec in gallery_specs(symbol)}
    df_1m = load_1m_source(str(data_path))
    df = load_and_resample(str(data_path), resample_to)
    weekly = build_weekly(df)

    _plot_weekday_distribution(weekly, symbol, "LOW", "Low_Weekday", output_dir / specs[f"{symbol.lower()}_weekly_low_day_distribution.png"].filename)
    _plot_weekday_distribution(weekly, symbol, "HIGH", "High_Weekday", output_dir / specs[f"{symbol.lower()}_weekly_high_day_distribution.png"].filename)
    _plot_hour_distribution(weekly, symbol, output_dir / specs[f"{symbol.lower()}_weekly_extreme_hour_distribution.png"].filename)
    _plot_day_session_heatmap(weekly, symbol, output_dir / specs[f"{symbol.lower()}_weekly_day_session_heatmap.png"].filename)
    _plot_forward_touch(df_1m, symbol, output_dir / specs[f"{symbol.lower()}_midnight_open_forward_touch_15m.png"].filename)
    _plot_relative_path(df_1m, symbol, output_dir / specs[f"{symbol.lower()}_relative_path_context_summary.png"].filename)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build README example PNGs from local futures parquet data.")
    parser.add_argument("--symbol", default=DEFAULT_SYMBOL, help="Symbol label for chart titles and filenames. Default: ES")
    parser.add_argument("--data", type=Path, default=DEFAULT_DATA_PATH, help="Path to 1-minute parquet data. Default: data/es_1m.parquet")
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR, help="Directory for README PNGs. Default: output/examples")
    parser.add_argument("--resample-to", default=DEFAULT_RESAMPLE_TO, help="Resample interval for weekly charts. Default: 1h")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    build_examples(args.symbol, args.data, args.output_dir, args.resample_to)


if __name__ == "__main__":
    main()
