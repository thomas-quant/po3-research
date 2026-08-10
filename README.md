# PO3 Research — ES/NQ Futures Path Results

Research toolkit for measuring ES and NQ futures path structure from 1-minute OHLCV data. The README is results-first: charts and numbers below summarize the current local ES/NQ sample. Methods, conventions, and module details are collapsed below.

This is research infrastructure, not a trading system. “Predictive” means conditional association vs baseline in the sample, not a standalone trade rule.

> **Results below are stale — regeneration pending.** The tracked charts and tables in
> `output/examples/` were generated on 2026-05-11. Since then the local parquet gained
> about two months of data, and the methodology was corrected in several places that
> move these numbers:
>
> | Change | Effect on the tables below |
> | --- | --- |
> | Weekday now rolls at 18:00 ET instead of the calendar day | Weekly-extreme weekday shares change; Monday no longer carries two overnight sessions |
> | Weekly targets restricted to Monday/Tuesday rows | The TWAP/VWAP residual-edge table was computed by pooling all weekdays, including Friday rows where the weekly label is contemporaneous |
> | "Next session" no longer means "day close" | Any next-session figure predates the fix |
> | `Day bullish` dropped for `Globex_Open` as tautological | That level's day-direction row disappears from the matrix |
>
> Re-run the command in [Reproduce These Results](#reproduce-these-results) for current
> numbers. The method descriptions further down describe the *current* code.

## Key Findings From Current ES/NQ Sample

### Weekly extremes skew toward Monday lows and Friday highs

| Symbol | Sample | Most common weekly low | Most common weekly high | Bullish week rate |
| --- | ---: | --- | --- | ---: |
| ES | All | Monday — 35.92% | Friday — 36.53% | 59.59% |
| ES | OOS | Monday — 43.10% | Friday — 40.52% | 59.48% |
| NQ | All | Monday — 36.89% | Friday — 34.83% | 58.62% |
| NQ | OOS | Monday — 40.52% | Friday — 35.34% | 57.76% |

Takeaway: both indices show the same broad path tendency in this sample: weekly lows form most often on Monday, weekly highs most often on Friday, with the pattern stronger in OOS.


### Globex→Midnight direction is early state, not post-midnight prediction

Positive Globex→Midnight change is a strong classifier for the full-day bullish label, but it does not meaningfully predict continuation after midnight.

| Symbol | Split | Full-day bullish baseline | If Midnight > Globex | If Midnight ≤ Globex | Midnight→Close positive baseline | If Midnight > Globex | If Midnight ≤ Globex |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: |
| ES | Train | 54.63% | 63.56% | 45.20% | 54.63% | 53.82% | 55.48% |
| ES | OOS | 55.41% | 62.76% | 47.06% | 52.29% | 50.69% | 54.12% |
| NQ | Train | 55.11% | 63.57% | 45.57% | 55.18% | 55.47% | 54.84% |
| NQ | OOS | 54.41% | 63.61% | 43.60% | 53.86% | 53.74% | 54.00% |

Takeaway: “Midnight above Globex” mostly says the day is already bullish relative to the Globex open. It is useful as an early day-state label, but the forward-only Midnight→Close test shows little/no continuation edge.

Note: the ES Train row repeats 54.63% in both baseline columns. Those are separate
quantities and recomputation puts them about a point apart, so that cell is a
transcription error. This table is now generated into `readme_findings_summary.csv`,
so the next regeneration will replace it.

### Midnight-open retaps remain common after the cash open

| Symbol | Train condition | Probability midnight open is touched later |
| --- | --- | ---: |
| ES | From 09:30 ET through same-day close | 65.72% |
| ES | From 13:00 ET through same-day close | 38.53% |
| NQ | From 09:30 ET through same-day close | 69.67% |
| NQ | From 13:00 ET through same-day close | 36.60% |

Takeaway: a large share of days still retap the NY midnight open after 09:30 ET. By 13:00 ET, retap probability drops but remains material.

### TWAP/VWAP needed a sanity check: same-level close state was mostly tautological

The first pass showed large deltas for “TWAP/VWAP above a key open → day closes above that same key open.” That is mostly a mechanical state check: if price spent the window above a level, closing above that level later is not much of a discovery.

The current matrix removes that headline target and scores only future/non-overlap outcomes. It also subtracts a simple sanity baseline: whether the window ended above or below the same level. Positive values below are the residual edge after that baseline.

| Symbol | Strongest OOS non-tautological level/window | Signal | Target | Best side | Residual edge |
| --- | --- | --- | --- | --- | ---: |
| ES | NY 13:00 Open / 13:00→Close | VWAP | Weekly high Friday | Below | 2.95 ppt |
| ES | Globex Open / Globex→Midnight | VWAP | Week bullish | Below | 2.74 ppt |
| ES | Globex Open / Globex→Midnight | VWAP | Weekly low Monday | Below | 2.33 ppt |
| NQ | NY 09:30 Open / 09:30→13:00 | TWAP | Week bullish | Below | 3.72 ppt |
| NQ | Globex Open / Globex→Midnight | TWAP | Week bullish | Below | 2.91 ppt |
| NQ | Globex Open / Globex→Midnight | VWAP | Week bullish | Below | 2.83 ppt |

Interpretation: once the obvious “where did the window close vs the level?” baseline is removed, remaining TWAP/VWAP edge is much smaller. Treat these as weak conditional structure candidates, not strong predictive findings.

![ES TWAP/VWAP predictive matrix](output/examples/es_twap_vwap_predictive_matrix.png)

![NQ TWAP/VWAP predictive matrix](output/examples/nq_twap_vwap_predictive_matrix.png)

## ES/NQ Result Gallery

| ES | NQ |
| --- | --- |
| ![ES weekly low weekday distribution](output/examples/es_weekly_low_day_distribution.png) | ![NQ weekly low weekday distribution](output/examples/nq_weekly_low_day_distribution.png) |
| ![ES weekly high weekday distribution](output/examples/es_weekly_high_day_distribution.png) | ![NQ weekly high weekday distribution](output/examples/nq_weekly_high_day_distribution.png) |
| ![ES weekly extreme hour distribution](output/examples/es_weekly_extreme_hour_distribution.png) | ![NQ weekly extreme hour distribution](output/examples/nq_weekly_extreme_hour_distribution.png) |
| ![ES weekly day session heatmap](output/examples/es_weekly_day_session_heatmap.png) | ![NQ weekly day session heatmap](output/examples/nq_weekly_day_session_heatmap.png) |
| ![ES midnight open forward-touch probability](output/examples/es_midnight_open_forward_touch_15m.png) | ![NQ midnight open forward-touch probability](output/examples/nq_midnight_open_forward_touch_15m.png) |
| ![ES relative path context summary](output/examples/es_relative_path_context_summary.png) | ![NQ relative path context summary](output/examples/nq_relative_path_context_summary.png) |

## Reproduce These Results

Expected local data:

- `data/es_1m.parquet`
- `data/nq_1m.parquet`

Install dependencies in a virtual environment:

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install pandas numpy matplotlib pyarrow pytest
```

Regenerate README artifacts for ES and NQ:

```bash
python3 scripts/build_readme_examples.py \
  --symbol-data ES=data/es_1m.parquet \
  --symbol-data NQ=data/nq_1m.parquet \
  --output-dir output/examples \
  --resample-to 1h
```

Generated summary tables:

- `output/examples/readme_findings_summary.csv` — every numbered finding above, including
  the Globex→Midnight state table, with the sample size behind each figure
- `output/examples/readme_twap_vwap_predictive_summary.csv` — the full TWAP/VWAP matrix,
  with `Weekday_Scope` and `n_weeks` per row

Run the research modules for a symbol:

```bash
# every module, ES
python3 -m po3_research --symbol ES --data data/es_1m.parquet

# a subset, NQ
python3 -m po3_research --symbol NQ --data data/nq_1m.parquet \
  --modules relative_path path_dependency
```

Modules: `weekly_charts`, `weekly_events`, `weekly_open_revisit`, `intraday_levels`,
`path_dependency`, `relative_path`, `monthly_extremes`, `monthly_levels`,
`month_context`. Output is written per symbol, so ES and NQ runs do not overwrite
each other. `python3 analysis.py` still works and accepts the same flags.

<details>
<summary><strong>How TWAP/VWAP predictive power is measured</strong></summary>

Only matched level/window pairs are scored — a level is never measured against a
window that closes before the level exists.

For each key level and its matching forward window:

| Key level | Window used for TWAP/VWAP | Main forward question |
| --- | --- | --- |
| Globex Open | Globex_to_Midnight | Does early Globex positioning relate to later session/day/week outcomes? |
| NY Midnight Open | Midnight_to_0930 | Does overnight positioning relate to 09:30→13:00, daily, or weekly outcomes? |
| NY 09:30 Open | 0930_to_1300 | Does AM positioning relate to 13:00→Close, daily, or weekly outcomes? |
| NY 13:00 Open | 1300_to_Close | Does PM positioning relate to close state, daily, or weekly outcomes? |

Signals:

- `TWAP_Above_Level`
- `VWAP_Above_Level`

Targets:

- next-session bullish %
- next-session positive return %
- day bullish %
- week bullish %
- weekly high Friday %
- weekly low Monday %

Scoring rules:

- **Weekly targets use Monday/Tuesday rows only** (`Weekday_Scope` column). Later in the
  week a weekly label describes the session being measured rather than following it.
- `n_weeks` is reported next to `n`. One weekly label repeats across up to five day-rows,
  so `n` overstates how many independent observations there are.
- A target identical to the level's own close state is dropped. For `Globex_Open` the
  level value is the day open, so "day bullish" and "day closes above the level" are the
  same column.
- A window with no following session (every `1300_to_Close` row) contributes no
  next-session observation. It is excluded, not scored as a negative.
- "Next session" is the next contiguous session block after the window, not the day close.

For each split, level, window, signal, and target:

```text
baseline = P(target)
raw_delta = P(target | TWAP/VWAP side) - baseline
end_state_delta = P(target | window close side vs level) - baseline
residual_edge = abs(raw_delta) - abs(end_state_delta)
```

The matrix cell shows positive residual edge in percentage points. Zero means TWAP/VWAP did not beat the simple end-state baseline.

</details>

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

## Session Date — One Definition Everywhere

The session date rolls at 18:00 ET. `trading_weekday(ts)` and `intraday_trading_day(ts)`
both use it: Sunday 18:00 and Monday 09:30 are both Monday, Monday 19:00 is Tuesday.

That matters for the weekly-extreme tables. Attributing bars by calendar day instead
gives each weekday a different amount of exposure:

| | Mon | Tue | Wed | Thu | Fri |
| --- | ---: | ---: | ---: | ---: | ---: |
| Calendar day, hours/week | 28.5 | 23.2 | 23.2 | 22.9 | 17.0 |
| Session date, hours/week | 23 | 23 | 23 | 23 | 23 |

Under calendar attribution Monday absorbs both Sunday's overnight session and its own,
while Friday stops at the 17:00 close — so a uniform null is 24.9% for Monday and 14.6%
for Friday, not 20% each. Session-date attribution removes that.

## Session Month

A month is a **session month**: it holds every bar whose session date falls in that
calendar month. August 2025 therefore opens at the 18:00 ET Globex print on 31 July
and closes at the 17:00 ET print on 29 August. `trading_month_start(ts)` returns the
calendar anchor, mirroring `trading_week_monday(ts)`.

The first and last months of the sample are truncated and are excluded from every
rate (`Is_Partial`).

Key opens use New York time.

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
- bootstrap confidence intervals, clustered on the week
- survival curves and remaining-event distributions

CIs and the sparse flag count independent weeks (`n_clusters`), not bars: a week
contributes ~120 correlated rows and exactly one event. `strongest_excess_distributions`
is still a top-10 shortlist out of ~75 cells with no multiple-comparison correction —
treat it as a place to look, not a result.

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

## 4. Intraday Key-Level Retaps

Studies daily key opens:

| Level | ET time |
| --- | --- |
| Globex open | 18:00 |
| NY midnight open | 00:00 |
| NY 09:30 open | 09:30 |
| NY 13:00 open | 13:00 |

Measures same-day revisit rates, first revisit timing, touch counts, close state, day direction, high/low already formed at revisit, and post-revisit excursions.

## 5. Forward-Touch Probabilities

For each key level and later bucket, estimates:

```text
P(level touched again from bucket start through same-day close)
```

Outputs include 15-minute and 1-hour bucket tables.

## 6. Intraday → Weekly Path Dependency

Links intraday key-level behavior to weekly outcomes using train p25/p75 buckets for minutes to revisit, touch count, and post-revisit excursions.

Summaries are keyed by weekday and report `n_weeks` alongside `n`. A weekly label is
forward-looking for a Monday row and contemporaneous for a Friday one, and one label
repeats across up to five day-rows.

## 7. Relative Path, TWAP/VWAP, and Composite Context

Measures time/distance above/below/touching each key level, window TWAP/VWAP, TWAP/VWAP distance to level, and composite state vs prior defined levels.

Every key level is crossed with every window, so 3 of the 16 combinations per day
describe a level that does not exist until after the window closes. Those rows carry
`Level_Defined_By_Window_End = False` and are excluded from the summaries by default
(`drop_lookahead_level_windows`); the detail CSV keeps them for inspection.

## 8. Monthly Extreme Timing

When in a session month the monthly high and low form. Primary cuts are session
position — thirds and quintiles of the month's ordered session list — so months
holding 13 to 24 sessions stay comparable. Week-of-month, weekday, session and
day-of-month are secondary views.

**Monthly results carry no train/OOS split and are labelled descriptive, not
validated.** The ES sample holds 194 months against 841 weeks; a 2023-12-31 split
leaves 31 OOS months, so a five-way conditional cut gives ~6 observations per cell.
Every figure instead carries `n`, a bootstrap 95% CI, and `is_sparse` against
`SPARSE_MONTHS = 20`. No "strongest cell" ranking table is produced.

The week-of-month table carries `months_present` and `avg_sessions`. W5 exists in
only about 41 of 192 months and spans ~2 sessions against W1–W4's ~5, so its share
is not comparable to theirs — and at n=41 the sparse flag alone would not say so.

## 9. Monthly Levels

`Monthly_Open`, `Prior_Month_High`, `Prior_Month_Low`, `Prior_Month_Close` — one
price each that stays live for a whole month, so they get their own builder rather
than an entry in `KEY_LEVEL_TIMES`.

Touch counting starts at 09:30 ET on the month's first RTH session
(`MONTHLY_LEVEL_COUNT_FROM`). Without that guard the monthly-open retap rate is
~100% and carries no information: the 18:00 print is set in thin hours and is
retested within minutes. This is the monthly analogue of the Monday 09:30 rule on
weekly-open revisits.

Forward-touch probability — level touched from session `k` through month end — is
reported on two axes: raw session index, where `n` falls away past ~19 sessions, and
normalized decile, where every month contributes to every bucket.

## 10. Month-State Context

Month-state attaches to the existing intraday and weekly rows as a conditioner:
month of year, session position in month, third, quintile, week of month,
month-to-date return, state versus the monthly open, and prior-month direction.
Weekly rows take their state from the Monday's session and carry a
`Straddles_Month_Boundary` flag; roughly a third of trading weeks span two months.

Two rules govern it:

- **Knowability.** A conditioner must be knowable at the time of the row it
  conditions. Prior-month and month-to-date features qualify; the month's eventual
  direction, high, low or close do not. Enforced by the `MONTH_STATE_CONDITIONERS`
  allow-list and a test asserting no whole-month label is in it.
- **Scope.** Monthly targets are scored on `Third_In_Month == Early` rows only,
  recorded in `Month_Scope`, with `n_months` beside `n`. A late-month row
  "predicting" its own month's close is describing it.

</details>

<details>
<summary><strong>Output layout</strong></summary>

Generated outputs are local artifacts and ignored by git, except tracked README examples in `output/examples/`.

```text
output/
├── examples/                              # tracked README result PNGs + summary CSVs
├── es/                                    # standard weekly charts + experiments, per symbol
├── nq/
└── research_events/
    ├── weekly_events_es/                  # one directory per module per symbol
    ├── weekly_open_revisit_es/
    ├── intraday_levels_es/
    ├── path_dependency_es/
    ├── relative_path_es/
    ├── monthly_extremes_es/
    ├── monthly_levels_es/
    ├── month_context_es/
    └── ..._nq/
```

</details>

<details>
<summary><strong>Tests and project files</strong></summary>

Run tests:

```bash
python3 -m pytest -q tests/
python3 -m py_compile analysis.py scripts/build_readme_examples.py po3_research/research.py
```

| Path | Purpose |
| --- | --- |
| `po3_research/research.py` | research implementation and CLI |
| `po3_research/__main__.py` | `python3 -m po3_research` entry point |
| `analysis.py` | backward-compatible runner/import wrapper |
| `scripts/build_readme_examples.py` | reproducible README result generator |
| `tests/test_event_research.py` | regression/unit tests for research helpers |
| `tests/test_readme_examples.py` | README artifact metadata and script-entry tests |
| `tests/test_cli.py` | CLI arguments, module selection, output scoping |
| `data/` | local parquet data, ignored by git |
| `output/examples/` | tracked README result artifacts |

</details>
