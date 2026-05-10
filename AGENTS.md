# AGENTS.md

Guidance for AI coding agents working in this repository.

## Project Purpose

This repository is a research toolkit for ES/NQ futures path structure. It analyzes:

- weekly high/low timing distributions
- online weekly extreme formation
- weekly-open revisits
- intraday key-level retaps
- forward-touch probabilities
- intraday path dependency vs weekly outcomes
- relative path features around key opens, including TWAP/VWAP and composite state

The project is research-first. Do not turn outputs into trading rules unless explicitly asked.

## Main Files

- `po3_research/research.py` — research implementation. `analysis.py` is a backward-compatible runner/import wrapper.
- `tests/test_event_research.py` — regression/unit tests for research helpers.
- `README.md` — user-facing overview and usage.
- `data/` — local parquet data, ignored by git.
- `output/` — generated charts/tables, ignored by git.

## Data

Expected local data:

- `data/es_1m.parquet`
- `data/nq_1m.parquet`

Current schema:

- `datetime_utc` — timezone-aware UTC timestamp
- `Open`, `High`, `Low`, `Close`, `Volume`

Legacy `DateTime_ET` and `DateTime_UTC` schemas are still supported by loader helpers.

Always convert timestamps to `America/New_York` before session, key-level, trading-day, or weekly grouping logic.

## Core Conventions

### Futures trading week

Use `trading_week_monday(ts)`. Do not use `pd.Grouper(freq="W-MON")`; it creates Tuesday→Monday buckets and misclassifies Monday extremes.

### Intraday trading day

Use `intraday_trading_day(ts)` / `intraday_trading_day_index(index)`:

- 18:00 ET and later belongs to the next RTH date.
- Key opens are New York time.

### Sessions

Defined in `SESSIONS`:

- Asia: 19:00–00:00
- London: 00:00–09:00
- NY AM: 09:00–12:00
- NY PM: 12:00–16:00
- Other: 16:00–19:00

### Train/OOS split

`TRAIN_END = 2023-12-31 America/New_York`.

Use train quantiles for p25/p75 buckets, then apply those thresholds to OOS.

## Research Modules in `po3_research/research.py`

- Weekly aggregation: `build_weekly`
- Standard weekly charts: `chart_*`
- Conditional experiments: `run_experiment`
- Event distribution: `build_event_rows`, `run_event_distribution_research`
- Weekly open revisits: `build_weekly_open_revisit_rows`, `run_weekly_open_revisit_research`
- Intraday key levels: `build_intraday_key_level_rows`, `run_intraday_key_level_research`
- Forward-touch probabilities: `intraday_level_forward_touch_distribution`
- Intraday→weekly dependency: `intraday_to_weekly_path_dependency`, `run_intraday_to_weekly_path_dependency_research`
- Relative path features: `build_relative_level_path_rows`, `run_relative_path_research`

## Key Levels

`KEY_LEVEL_TIMES`:

- `Globex_Open`: 18:00 ET
- `NY_Midnight_Open`: 00:00 ET
- `NY_0930_Open`: 09:30 ET
- `NY_1300_Open`: 13:00 ET

## Relative Path Windows

`RELATIVE_WINDOWS`:

- `Globex_to_Midnight`: 18:00 → 00:00
- `Midnight_to_0930`: 00:00 → 09:30
- `0930_to_1300`: 09:30 → 13:00
- `1300_to_Close`: 13:00 → 17:00

Relative path features include time above/below/touching, distance above/below, TWAP/VWAP distance, and composite close state vs prior defined levels.

## Before Claiming Completion

Run:

```bash
python3 -m pytest -q tests/test_event_research.py
python3 -m py_compile analysis.py
```

If changing generated-output behavior, run the relevant research function on ES and/or NQ and inspect produced CSVs.

## Git Hygiene

- Do not commit data files.
- Do not commit generated output.
- Keep `AGENTS.md` tracked.
- `CLAUDE.md` is ignored; use `AGENTS.md` for shared agent instructions.
