# PO3 Research — Futures Path & Weekly Extreme Analysis

Research toolkit for studying ES and NQ futures structure from 1-minute OHLCV data. It measures weekly high/low timing, online extreme formation, weekly-open revisits, intraday key-level retaps, forward-touch probabilities, intraday → weekly path dependency, and relative path context around key opens.

This repository is research infrastructure, not a trading system. Outputs are descriptive and conditional distributions for reasoning about futures path structure.

## Quick Start

```bash
python3 analysis.py
```

Default config lives in `po3_research/research.py` and is re-exported by `analysis.py`:

```python
SYMBOL = "ES"
DATA_PATH = "data/es_1m.parquet"
RESAMPLE_TO = "1h"
OUTPUT_DIR = Path("output")
```

Expected local data:

- `data/es_1m.parquet`
- `data/nq_1m.parquet`

Install dependencies in a virtual environment:

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install pandas numpy matplotlib pyarrow pytest
```

## README Example Gallery

These tracked images are generated from local ES data with `scripts/build_readme_examples.py`.

| Research view | Example |
| --- | --- |
| Weekly low weekday distribution | ![ES weekly low weekday distribution](output/examples/es_weekly_low_day_distribution.png) |
| Weekly high weekday distribution | ![ES weekly high weekday distribution](output/examples/es_weekly_high_day_distribution.png) |
| Weekly extreme hour distribution | ![ES weekly extreme hour distribution](output/examples/es_weekly_extreme_hour_distribution.png) |
| Day × session timing heatmap | ![ES weekly day session heatmap](output/examples/es_weekly_day_session_heatmap.png) |
| Midnight-open forward-touch probability | ![ES midnight open forward-touch probability](output/examples/es_midnight_open_forward_touch_15m.png) |
| Relative path context before 09:30 | ![ES relative path context summary](output/examples/es_relative_path_context_summary.png) |

Regenerate the gallery:

```bash
python3 scripts/build_readme_examples.py \
  --symbol ES \
  --data data/es_1m.parquet \
  --output-dir output/examples \
  --resample-to 1h
```

<details>
<summary><strong>Data schema and time conventions</strong></summary>

## Data Schema

Current schema is UTC-first:

| Column | Notes |
| --- | --- |
| `datetime_utc` | timezone-aware UTC timestamp |
| `Open`, `High`, `Low`, `Close`, `Volume` | 1-minute OHLCV |

Legacy schemas with `DateTime_ET` / `DateTime_UTC` are still supported by loader helpers.

## Timezone

All session, key-level, trading-day, and weekly grouping logic converts source timestamps to `America/New_York`.

## Futures Trading Week

Use `trading_week_monday(ts)`. Do not use `pd.Grouper(freq="W-MON")`; it creates Tuesday → Monday buckets and misclassifies Monday extremes.

## Intraday Trading Day

Use `intraday_trading_day(ts)` or `intraday_trading_day_index(index)`:

- 18:00 ET and later belongs to the next RTH date.
- Key opens use New York time.

## Sessions

| Session | ET hours |
| --- | --- |
| Asia | 19:00–00:00 |
| London | 00:00–09:00 |
| NY AM | 09:00–12:00 |
| NY PM | 12:00–16:00 |
| Other | 16:00–19:00 |

## Train/OOS Split

`TRAIN_END = 2023-12-31 America/New_York`.

Train quantiles define p25/p75 buckets. Those fixed thresholds then apply to OOS rows.

</details>

<details>
<summary><strong>Research modules</strong></summary>

## 1. Weekly Extreme Timing

Builds Monday-anchored futures weeks and measures when the weekly high/low forms.

Outputs include:

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

## 2. Online Weekly Event Distribution

Builds one row per week × bar to study whether weekly high/low has formed yet, using only path-state features known at that point.

Features include:

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

## 3. Weekly-Open Revisits

Measures when price revisits the weekly open.

Definition:

- weekly open = first 1-minute bar open of the trading week
- revisit = `Low <= weekly_open <= High`
- Sunday evening excluded from revisit counting
- Monday only counts from 09:30 ET onward
- timing buckets learned from train quantiles, then validated on OOS

Function:

- `build_weekly_open_revisit_rows(df_1m, weekly=None)`

## 4. Intraday Key-Level Retaps

Studies daily key opens:

| Level | ET time |
| --- | --- |
| Globex open | 18:00 |
| NY midnight open | 00:00 |
| NY 09:30 open | 09:30 |
| NY 13:00 open | 13:00 |

Measures:

- same-day revisit rate
- first revisit session/hour
- total touch bars by session
- day close above/below level
- day bullish/bearish state
- whether high/low already formed at revisit
- post-revisit high/low excursions

Output path:

```text
output/research_events/intraday_levels/
```

## 5. Forward-Touch Probabilities

For each key level and later bucket, estimates:

```text
P(level touched again from bucket start through same-day close)
```

Examples:

- probability midnight open is touched after 09:30
- probability 09:30 open is touched after 13:00
- probability 13:00 open is touched after 15:00

Outputs include 15-minute and 1-hour bucket tables.

## 6. Intraday → Weekly Path Dependency

Links intraday key-level behavior to weekly outcomes.

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

## 7. Relative Path, TWAP/VWAP, and Composite Context

Measures how much time/distance price spends around key levels during pre-session windows.

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
- composite state vs prior defined levels:
  - `Above_All`
  - `Below_All`
  - `Between`

Summaries test whether path context relates to:

- next-session direction/return
- daily close above/below level
- daily bullish/bearish close
- Monday/Tuesday path → weekly high/low distribution

Output paths:

```text
output/research_events/relative_path_es/
output/research_events/relative_path_nq/
```

</details>

<details>
<summary><strong>Output layout</strong></summary>

Generated outputs are local artifacts and ignored by git, except tracked README examples in `output/examples/`.

```text
output/
├── examples/                              # tracked README gallery PNGs
├── *.png                                  # standard weekly charts
└── research_events/
    ├── intraday_levels/
    ├── intraday_levels_nq/
    ├── path_dependency_es/
    ├── path_dependency_nq/
    ├── relative_path_es/
    └── relative_path_nq/
```

</details>

<details>
<summary><strong>Common commands</strong></summary>

Run the default ES research pass:

```bash
python3 analysis.py
```

Run tests:

```bash
python3 -m pytest -q tests/test_event_research.py tests/test_readme_examples.py
python3 -m py_compile analysis.py scripts/build_readme_examples.py
```

Regenerate README images:

```bash
python3 scripts/build_readme_examples.py --data data/es_1m.parquet
```

Run NQ-specific research by changing `SYMBOL` and `DATA_PATH`, or by calling research functions directly with `data/nq_1m.parquet`.

</details>

<details>
<summary><strong>Project files</strong></summary>

| Path | Purpose |
| --- | --- |
| `po3_research/research.py` | research implementation |
| `analysis.py` | backward-compatible runner/import wrapper |
| `scripts/build_readme_examples.py` | reproducible README gallery generator |
| `tests/test_event_research.py` | regression/unit tests for research helpers |
| `tests/test_readme_examples.py` | gallery metadata and script-entry tests |
| `data/` | local parquet data, ignored by git |
| `output/` | generated charts/tables, mostly ignored by git |

</details>
