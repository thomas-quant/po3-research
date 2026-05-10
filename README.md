# PO3 Research — Futures Path & Weekly Extreme Analysis

Research toolkit for studying ES and NQ futures structure from 1-minute OHLCV data. The project started as weekly high/low timing research and now includes intraday key-level retaps, forward-touch probabilities, path dependency, TWAP/VWAP context, and early-week signals that relate intraday behavior to weekly distributions.

Data is intentionally not tracked in git. Expected local files:

- `data/es_1m.parquet`
- `data/nq_1m.parquet`

Current data schema is UTC-first:

| Column | Notes |
| --- | --- |
| `datetime_utc` | timezone-aware UTC timestamp |
| `Open`, `High`, `Low`, `Close`, `Volume` | 1-minute OHLCV |

Legacy schemas with `DateTime_ET` / `DateTime_UTC` are still supported.

---

## Quick Start

```bash
python3 analysis.py
```

Default config in `analysis.py`:

```python
SYMBOL = "ES"
DATA_PATH = "data/es_1m.parquet"
RESAMPLE_TO = "1h"
OUTPUT_DIR = Path("output")
```

To run NQ, change `SYMBOL` and `DATA_PATH`, or call the research functions directly with `data/nq_1m.parquet`.

Dependencies:

```bash
pip install pandas numpy matplotlib pyarrow pytest
```

---

## What This Research Measures

### 1. Weekly extreme timing

Builds Monday-anchored futures weeks and measures when the weekly high/low forms:

- weekday distribution
- session distribution
- hour distribution
- day × session heatmaps
- bullish/bearish week conditioning
- prior-week direction experiments

Core functions:

- `load_and_resample(path, resample_to)`
- `build_weekly(df)`
- `run_experiment(...)`

### 2. Online weekly event distribution

Builds one row per week × bar to study whether weekly high/low has formed yet, using only path-state features known at that point:

- developing weekly range
- close location in developing range
- return from weekly open
- prior high/low break state
- range expansion bucket
- train/OOS split
- bootstrap confidence intervals
- survival curves and remaining-event distributions

Output path:

```text
output/research_events/
```

### 3. Weekly-open revisits

Uses 1-minute data to measure when price revisits the weekly open.

Definition:

- weekly open = first 1-minute bar open of the trading week
- revisit = `Low <= weekly_open <= High`
- Sunday evening excluded from revisit counting
- Monday only counts from 09:30 ET onward
- timing buckets are learned from train quantiles, with OOS validation

### 4. Intraday key-level retaps

Studies daily key opens:

| Level | ET time |
| --- | --- |
| Globex open | 18:00 |
| NY midnight open | 00:00 |
| NY 09:30 open | 09:30 |
| NY 13:00 open | 13:00 |

Trading day convention: 18:00 ET belongs to the next RTH date.

Measures:

- whether each level is revisited same trading day
- first revisit session/hour
- total touch bars by session
- day close above/below level
- day bullish/bearish
- high/low already formed at revisit
- post-revisit excursions

Output path:

```text
output/research_events/intraday_levels/
```

### 5. Forward-touch probabilities

For each key level and later bucket, estimates:

```text
P(level touched again from bucket start through same-day close)
```

Examples:

- probability midnight open is touched after 09:30
- probability 09:30 open is touched after 13:00
- probability 13:00 open is touched after 15:00

Outputs include 15-minute and 1-hour bucket tables.

### 6. Intraday → weekly path dependency

Links intraday behavior around key levels to weekly outcomes.

Metrics bucketed with train p25/p75 quantiles:

- minutes to first revisit
- touch-bar count
- post-revisit high/low excursion

Weekly outcomes:

- week bullish %
- weekly high Friday %
- weekly low Monday %
- weekly high/low formed by that day %

Output paths:

```text
output/research_events/path_dependency_es/
output/research_events/path_dependency_nq/
```

### 7. Relative path, TWAP/VWAP, and composite context

Measures how much time/distance price spends around key levels during pre-session windows:

| Window | ET range |
| --- | --- |
| Globex to Midnight | 18:00 → 00:00 |
| Midnight to 09:30 | 00:00 → 09:30 |
| 09:30 to 13:00 | 09:30 → 13:00 |
| 13:00 to Close | 13:00 → 17:00 |

Features:

- % bars above/below/touching each level
- mean/max distance above/below level
- window TWAP and VWAP
- TWAP/VWAP distance to level
- composite state vs all prior defined levels:
  - `Above_All`
  - `Below_All`
  - `Between`

Summaries test whether path context predicts:

- next-session direction/return
- daily close above/below level
- daily bullish/bearish close
- Monday/Tuesday path → weekly high/low distribution

Output paths:

```text
output/research_events/relative_path_es/
output/research_events/relative_path_nq/
```

---

## Time & Session Handling

All research is expressed in New York time. UTC source data is converted to `America/New_York`.

Sessions:

| Session | ET hours |
| --- | --- |
| Asia | 19:00–00:00 |
| London | 00:00–09:00 |
| NY AM | 09:00–12:00 |
| NY PM | 12:00–16:00 |
| Other | 16:00–19:00 |

Trading week handling is custom. Do not use `pd.Grouper(freq="W-MON")`; it creates Tuesday→Monday buckets. Use `trading_week_monday(ts)` instead.

---

## Important Outputs

Generated outputs are local artifacts and ignored by git.

```text
output/
├── *.png                                  # standard weekly charts
└── research_events/
    ├── intraday_levels/
    ├── intraday_levels_nq/
    ├── path_dependency_es/
    ├── path_dependency_nq/
    ├── relative_path_es/
    └── relative_path_nq/
```

---

## Tests

```bash
python3 -m pytest -q tests/test_event_research.py
python3 -m py_compile analysis.py
```

The tests cover:

- UTC/ET data loading
- futures week and trading-day mapping
- weekly event rows
- weekly-open revisits
- intraday key-level revisits
- forward-touch probabilities
- path-dependency buckets
- relative path TWAP/VWAP/composite features

---

## Notes

This is research infrastructure, not a trading system. Outputs are descriptive/conditional distributions intended to help reason about futures path structure, not standalone trade signals.
