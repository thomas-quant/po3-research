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

- `po3_research/research.py` — research implementation and CLI.
- `po3_research/__main__.py` — `python3 -m po3_research` entry point.
- `analysis.py` — backward-compatible runner/import wrapper.
- `scripts/build_readme_examples.py` — reproducible README result generator.
- `tests/` — regression tests for research helpers, README artifacts, and the CLI.
- `data/` — local parquet data, ignored by git.
- `output/` — generated charts/tables, ignored by git except `output/examples/`.

## Running

```bash
python3 -m po3_research --symbol ES --data data/es_1m.parquet
python3 -m po3_research --symbol NQ --data data/nq_1m.parquet --modules relative_path path_dependency
```

Modules: `weekly_charts`, `weekly_events`, `weekly_open_revisit`, `intraday_levels`,
`path_dependency`, `relative_path`. Each writes to a symbol-scoped directory, so ES
and NQ runs never overwrite each other.

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

### One day definition

The session date rolls at **18:00 ET**. `trading_weekday(ts)` and
`intraday_trading_day(ts)` both use it, so Sunday 18:00 and Monday 09:30 are both
Monday, and Monday 19:00 is Tuesday.

Do not reintroduce calendar-day attribution for weekly extremes. It gives Monday
~28.5h per week (its own evening plus Sunday's) against Friday's ~17h, which makes
the weekday buckets non-comparable and inflates Monday as the weekly-low day.

Vectorized twins exist for every hot path: `trading_weekday_index`,
`intraday_trading_day_index`, `trading_week_monday_index`, `session_of_index`. Use
them instead of `.map()` over a python helper — the per-bar versions cost minutes on
a 5.6M-bar sample.

### Futures trading week

Use `trading_week_monday(ts)`. Do not use `pd.Grouper(freq="W-MON")`; it creates Tuesday→Monday buckets and misclassifies Monday extremes.

### Sessions

Defined in `SESSIONS`:

- Asia: 19:00–00:00
- London: 00:00–09:00
- NY AM: 09:00–12:00
- NY PM: 12:00–16:00
- Other: 16:00–19:00

Sessions are not unique within a futures day — "Other" covers both the 16:00–18:00
tail and the 18:00 open — so "the next session" means the next *contiguous* block
(`_next_session_bounds`), never "the last bar with a different label".

### Train/OOS split

`TRAIN_END = 2023-12-31 America/New_York`.

Use train quantiles for p25/p75 buckets, then apply those thresholds to OOS. This
applies to every bucketing helper, including `_range_expansion_buckets`.

## Research Hazards

These have all been fixed once. Do not reintroduce them.

1. **Look-ahead level/window pairs.** `build_relative_level_path_rows` crosses every
   key level with every window, so 3 of 16 combinations per day measure bars against
   a level defined hours later. Rows carry `Level_Defined_By_Window_End`; summaries
   call `drop_lookahead_level_windows` first.
2. **Weekly labels on late-week rows.** A weekly outcome is forward-looking for a
   Monday row and contemporaneous for a Friday one. Weekly targets are keyed by
   weekday (`intraday_to_weekly_path_dependency`) or restricted to Mon/Tue
   (`relative_path_to_weekly_outcomes`, `_build_twap_vwap_summary`).
3. **Inflated n.** One weekly label repeats across up to 5 day-rows. Report
   `n_weeks` next to `n` whenever the target is weekly.
4. **Tautological targets.** For `Globex_Open` the level value *is* the day open, so
   `Day_Bullish` and `Day_Close_Above_Level` are the same column. Detect this by
   comparing values (`_is_same_level_close_state`), not by target name.
5. **NaN as a negative observation.** `Next_Session_Return_Pct > 0` scores "no
   following session" as False. Every `1300_to_Close` row is in that state.
6. **Correlated bootstrap samples.** `bootstrap_probability_ci` clusters on
   `Week_Start` when it is available, and `is_sparse` counts weeks (`n_clusters`),
   not bars. Keep it that way: ~120 correlated bars per week share one event, so a
   row-level bootstrap reports a CI several times too narrow.
7. **Multiple comparisons.** `strongest_excess_distributions` is the top 10 of ~75
   cells with no correction. It is a shortlist, not a result.

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
python3 -m pytest -q tests/
python3 -m py_compile analysis.py scripts/build_readme_examples.py po3_research/research.py
```

If changing generated-output behavior, run the relevant module on ES and/or NQ and inspect produced CSVs.

## Git Hygiene

- Do not commit data files.
- Do not commit generated output beyond `output/examples/`.
- Keep `AGENTS.md` tracked.
- `CLAUDE.md` is ignored; use `AGENTS.md` for shared agent instructions.
