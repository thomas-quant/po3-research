# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Quick Start

### Run Main Analysis
```bash
python analysis.py
```

This loads parquet data (configured in CONFIG block), resamples it to hourly, builds weekly summaries, and generates 4 standard charts + 5 experiments. Output saved to `output/`.

**To change symbol/data:**
Edit CONFIG in `analysis.py`:
```python
SYMBOL      = "ES"  # or "NQ"
DATA_PATH   = "data/es_1m.parquet"
RESAMPLE_TO = "1h"  # or "4h", "1D", etc.
```

## Architecture Overview

### Data Pipeline

**Input:** 1-minute OHLCV parquet files (ES/NQ, 2020–2025)
- Columns: `DateTime_ET` (naive, must localize), `Open`, `High`, `Low`, `Close`, `Volume`, `DateTime_UTC`, `session`, `window`
- Critical: `DateTime_ET` has no timezone info; must call `tz_localize("America/New_York", ambiguous="infer")` on it

**Processing Steps:**
1. `load_and_resample()` — Load parquet, localize ET, resample OHLCV to target freq
2. `build_weekly()` — Group into Mon–Fri weeks, extract extremes (day/session/hour)
3. Chart functions — Generate matplotlib visualizations
4. `run_experiment()` — Test predictors (does X predict Y?)

### Key Data Structures

**Weekly DataFrame** (from `build_weekly()`):
- `Bull_Bear` — "Bullish" or "Bearish" (close > open for week)
- `Low_Weekday`, `High_Weekday` — Day name when extreme formed
- `Low_Session`, `High_Session` — Session when extreme formed
- `Low_Hour`, `High_Hour` — Hour (ET) when extreme formed
- `Prev_Bull_Bear` — Previous week's direction

### Trading Week Handling (Critical)

**The Problem:** Pandas `W-MON` groups create Tuesday→Monday buckets, placing Monday as the last day. This inflates Monday as the extreme day and leaks Sunday 18:00+ (CME open) into wrong weeks.

**The Solution:** Custom functions in `analysis.py`:
- `trading_week_monday(ts)` — Maps Sunday 18:00+ to next Monday; returns Monday midnight of the week
- `trading_weekday(ts)` — Returns day name; Sunday evening → "Monday"

These ensure correct Mon–Fri buckets and proper Sunday evening classification.

### Session Definitions

Sessions are defined by ET hour in CONFIG:
```python
SESSIONS = {
    "Asia"   : (19, 24),   # 19:00 – midnight
    "London" : ( 0,  9),   # 00:00 – 09:00
    "NY AM"  : ( 9, 12),   # 09:00 – 12:00
    "NY PM"  : (12, 16),   # 12:00 – 16:00
    "Other"  : (16, 19),   # 16:00 – 19:00
}
```

Function `session_of(ts)` maps a timestamp to its session.

### Chart Generation Pattern

Each chart function:
1. Filters weekly data by Bullish/Bearish
2. Computes value_counts with normalization
3. Uses `_save(fig, name)` to save to `OUTPUT_DIR`

Helpers:
- `_save(fig, name)` — Save matplotlib figure to `output/{name}.png` at 150 DPI
- `_label_bars(ax, min_pct)` — Add percentage labels to bars ≥ min_pct
- `_subtitle(ax, text)` — Add subtitle below title

### Experiment Framework

`run_experiment(weekly, factor_col, target_col, factor_order=None, target_order=None, title=None, filename=None)`

Creates side-by-side subplots, one per unique factor value. Each subplot shows:
- Bar chart: P(target | factor = x)
- Dashed line: P(target) baseline

Useful for testing hypotheses:
```python
# Does week direction predict when LOW forms?
run_experiment(weekly, "Bull_Bear", "Low_Weekday",
               factor_order=["Bullish", "Bearish"],
               target_order=DAYS)

# Does previous week predict this week?
run_experiment(weekly, "Prev_Bull_Bear", "Bull_Bear")
```

## Code Organization

- **lines 1–42:** Module docstring, imports, CONFIG
- **lines 44–73:** `load_and_resample()` — data loading pipeline
- **lines 76–113:** `trading_weekday()`, `trading_week_monday()`, `session_of()` — time helpers
- **lines 115–154:** `build_weekly()` — weekly aggregation
- **lines 157–184:** Chart utilities (`_save`, `_subtitle`, `_label_bars`)
- **lines 187–334:** Standard analyses (`chart_day_distribution`, `chart_session_distribution`, etc.)
- **lines 337–453:** `run_experiment()` framework
- **lines 456–533:** `main` — runs all charts and experiments

## Customization Patterns

### Add a Custom Experiment
Uncomment in main, or add:
```python
run_experiment(weekly, "Low_Session", "High_Weekday",
               factor_order=SESSION_ORDER, target_order=DAYS,
               title="Session → High day",
               filename="custom_exp_1")
```

### Add a Custom Chart Function
Define a function that takes `weekly: pd.DataFrame`, use the chart utilities, call `_save()`.

### Change Resample Frequency
CONFIG: `RESAMPLE_TO = "4h"` (or `"1D"`, `"2h"`, etc.)

## Key Insights from Analysis

- **273 weeks** analyzed (2020–2025, ES data)
- **Bullish weeks:** LOW ~59% Monday, HIGH ~61% Friday
- **Bearish weeks:** HIGH ~45% Monday, LOW ~56% Friday
- Sessions/hours show clear patterns; Asia and NY AM dominate extremes

## Dependencies

- `pandas` — DataFrames, resampling, groupby
- `numpy` — Numerical operations
- `matplotlib` — Chart generation
- `pyarrow` — Parquet file I/O
- `pathlib` — Path handling (stdlib)

Install with: `pip install pandas numpy matplotlib pyarrow`

## Common Edits

| Task | Where |
|------|-------|
| Change data source | CONFIG: `DATA_PATH`, `SYMBOL` |
| Change resample freq | CONFIG: `RESAMPLE_TO` |
| Add experiment | Main, after line 485 |
| Modify session hours | CONFIG: `SESSIONS` dict |
| Change chart save dir | CONFIG: `OUTPUT_DIR` |
| Add/remove standard charts | Main: comment out calls to `chart_*()` functions |

## File Paths

- Parquet data: `data/es_1m.parquet`, `data/nq_1m.parquet`
- Charts output: `output/*.png`

---

**Last Updated:** March 2026
