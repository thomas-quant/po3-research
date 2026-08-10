# Monthly PO3 Design

## Goal

Extend the ES/NQ path-structure toolkit with monthly analysis: when in a month the
monthly high and low form, how the monthly open and prior-month levels behave as
intraday reference prices, and whether month-state conditions the existing weekly and
intraday results.

"PO3" here is the project name. The literal three-phase accumulation / manipulation /
distribution structure is explicitly out of scope for this spec.

## Sampling Reality

The ES sample spans 2010-06-06 → 2026-07-17: **194 months, 192 complete**, against 841
weeks. Months hold 13–24 sessions, median 21.

This drives one non-negotiable decision: **monthly outputs carry no train/OOS split.**
A 2023-12-31 split leaves 31 OOS months, so a five-way conditional cut gives ~6
observations per cell. Reporting that as out-of-sample validation would manufacture
confidence that the sample cannot support.

Instead every monthly figure carries `n`, a bootstrap 95% CI, and an `is_sparse` flag
against `SPARSE_MONTHS = 20`. Monthly results are labelled descriptive, not validated.
No "strongest cell" ranking table is produced for monthly outputs — with ~16
observations per month-of-year, a top-N of a wide grid is noise mining.

Because `build_monthly` emits exactly one row per month, the month *is* the independent
unit and `bootstrap_probability_ci` applies with no `clusters` argument. Clustering is
only needed where one label is spread across many correlated rows.

## Month Boundary

A month is a **session month**: it contains every bar whose session date
(`intraday_trading_day`, rolling at 18:00 ET) falls in that calendar month. August 2025
therefore opens at the 18:00 ET Globex print on 31 July and closes at the 17:00 ET print
on 29 August.

This mirrors the weekly convention exactly — the trading week opens Sunday 18:00 and is
labelled by the following Monday — and inherits the DST-safe date arithmetic in
`_shift_days_local`.

`Monthly_Open` is the open of the month's first bar, i.e. that 18:00 print.

### New helpers

```python
trading_month_start(ts)          -> first calendar day of the session month, ET midnight
trading_month_start_index(index) -> vectorized twin
```

Both route through `_shift_days_local`, so the DST fix applies. The label is the
calendar anchor, not the first bar, matching `trading_week_monday`.

## Component 1 — `build_monthly(df)`

One row per month.

| Field | Notes |
| --- | --- |
| `Month_Start`, `Month_Of_Year` | label and seasonality cut |
| `N_Sessions` | 13–24; the exposure denominator |
| `Is_Partial` | first and last month in the sample; excluded from every rate |
| `Bull_Bear`, `Prev_Bull_Bear` | close vs the 18:00 open; prior month's direction |
| `Monthly_Open`, `Monthly_Close`, `Monthly_High`, `Monthly_Low` | |
| `High_Session_Index`, `Low_Session_Index` | 0-based position in the month's ordered session list |
| `High_Session_Pos`, `Low_Session_Pos` | index ÷ (N_Sessions − 1); comparable across month lengths |
| `High_Third`, `Low_Third` | Early / Mid / Late; bucket = `floor(index / N_Sessions * 3)` |
| `High_Quintile`, `Low_Quintile` | Q1–Q5; bucket = `floor(index / N_Sessions * 5)` |
| `High_Week_Of_Month`, `Low_Week_Of_Month` | W1–W5 on calendar days; secondary view |
| `High_Weekday`, `High_Session`, `High_Hour`, `High_Day_Of_Month` | and the `Low_` equivalents |

Partial months are excluded from all rates. The sample begins mid-June 2010 and ends
mid-July 2026; both would otherwise contribute a truncated high/low.

Weekday, session and hour reuse the existing session-date helpers unchanged.

## Component 2 — Monthly extreme timing

Primary cuts are on session position (thirds and quintiles). Secondary cuts are
week-of-month, weekday, session, and day-of-month.

Every output carries `n`, `pct`, `ci_low`, `ci_high`, `is_sparse`, plus a
bullish/bearish split where n allows.

The week-of-month table carries two extra columns: `months_present` and `avg_sessions`.
W5 appears in only ~41 of 192 months and covers ~2 sessions against W1–W4's ~5. At n=41
it sits above the `SPARSE_MONTHS` floor, so the flag alone would not catch it — the
exposure columns are what make its share non-comparable visible. This is the
generalization of the fix applied to the weekly weekday buckets: a share means nothing
without knowing how much of the period the bucket spans, and how often it exists at all.

Outputs to `research_events/monthly_extremes_<symbol>/`:

```
monthly_rows.csv
high_timing_by_third.csv          low_timing_by_third.csv
high_timing_by_quintile.csv       low_timing_by_quintile.csv
high_timing_by_week_of_month.csv  low_timing_by_week_of_month.csv
high_timing_by_weekday.csv        low_timing_by_weekday.csv
high_timing_by_session.csv        low_timing_by_session.csv
high_timing_by_day_of_month.csv   low_timing_by_day_of_month.csv
```

Charts for the primary cuts (thirds, quintiles) and for week-of-month.

## Component 3 — Monthly levels

A monthly level is one price that stays live for a whole month, so it cannot be
expressed as an entry in `KEY_LEVEL_TIMES`, which is keyed by time of day. It gets its
own builder.

```python
MONTHLY_LEVELS = ["Monthly_Open", "Prior_Month_High", "Prior_Month_Low", "Prior_Month_Close"]
```

`build_monthly_level_rows(df_1m)` emits one row per month × level:

| Field | Notes |
| --- | --- |
| `Level_Value` | |
| `Touched` | any eligible bar with `Low <= value <= High` |
| `First_Touch_Timestamp` | and its session index, normalized position, weekday, session |
| `Touch_Sessions` | count of distinct sessions containing at least one touch |
| `Touch_Bars_Total` | count of touching 1-minute bars |
| `Month_Close_Above_Level` | |
| `Post_Touch_High_Excursion`, `Post_Touch_Low_Excursion` | from first touch to month end |

### Eligibility guard

Touch counting starts at **09:30 ET on the month's first RTH session**, exposed as a
named constant (`MONTHLY_LEVEL_COUNT_FROM = (9, 30)`).

Without it the monthly-open retap rate is ~100% and carries no information: the 18:00
print is set in thin hours and is retested within minutes. This is the monthly analogue
of `_is_weekly_open_revisit_eligible`, which starts weekly-open counting at Monday 09:30
for the same reason.

Prior-month levels are known before the month opens and use the same guard for
comparability.

### Forward touch

For each month × level × starting point, the probability the level is touched from that
point through month end, where "starting point k" means from the first eligible bar of
session k onward. Two axes:

- **raw session index** — directly interpretable; n falls away past ~19 sessions, which
  the output records
- **normalized decile** — every month contributes to every bucket, so the curve is
  comparable across month lengths

Implementation reuses the reverse `np.maximum.accumulate` trick from
`intraday_level_forward_touch_distribution`, applied per month.

Outputs to `research_events/monthly_levels_<symbol>/`:

```
monthly_level_rows.csv
monthly_level_touch_distribution.csv
monthly_level_first_touch_by_third.csv
monthly_level_forward_touch_by_session_index.csv
monthly_level_forward_touch_by_decile.csv
```

## Component 4 — Month-state as a conditioner

Month-state attaches to the existing day-level row builders
(`build_intraday_key_level_rows`, `build_relative_level_path_rows`):

- `Month_Of_Year`
- `Session_Pos_In_Month`, `Third_In_Month`, `Quintile_In_Month`, `Week_Of_Month`
- `Month_Return_So_Far_Pct` — monthly open to the current session's close
- `Above_Monthly_Open` — state at the current session
- `Prior_Month_Bull_Bear`

Weekly rows take month-state from their Monday's session date, plus a
`Straddles_Month_Boundary` flag: roughly a third of trading weeks span two months.

### The knowability rule

**A month-state conditioner must be knowable at the time of the row it conditions.**
Prior-month features and month-to-date path features qualify. The month's eventual
direction, high, low, or close do not. Conditioning a daily outcome on the whole month's
label repeats the contemporaneous-label defect removed from the weekly targets.

This is enforced by an explicit allow-list of conditioner columns plus a test asserting
that no whole-month label appears in it — the same guard style as
`_is_same_level_close_state`.

### The scope rule for monthly targets

The mirror case — does early-month behaviour predict the monthly close — is legitimate,
but only for early-month rows. Monthly targets are scored on `Third_In_Month == Early`
rows only, recorded in a `Month_Scope` column, and carry `n_months` beside `n`.

This is the session-position generalization of the weekday scope rule already applied to
weekly targets in `_build_twap_vwap_summary` and
`intraday_to_weekly_path_dependency`. A late-month row "predicting" its own month's
close is describing it.

Outputs to `research_events/month_context_<symbol>/`:

```
intraday_outcomes_by_month_state.csv
weekly_outcomes_by_month_state.csv
```

## Architecture

Everything lands in `po3_research/research.py` in clearly delimited sections, following
the existing monolith layout. Three new CLI modules:

```bash
python3 -m po3_research --symbol ES --modules monthly_extremes monthly_levels month_context
```

`MODULES` becomes:

```python
["weekly_charts", "weekly_events", "weekly_open_revisit", "intraday_levels",
 "path_dependency", "relative_path", "monthly_extremes", "monthly_levels", "month_context"]
```

Each writes to its own symbol-scoped directory via `module_output_dir`, so ES and NQ
runs do not collide.

`monthly_extremes` runs off the 1h resampled frame, as the weekly charts do.
`monthly_levels` and `month_context` need 1-minute bars. With 194 month groups and
vectorized touch detection, both should cost seconds rather than the minutes the
per-day loops take.

## Testing

Tests target the hazards, not the happy path:

1. `trading_month_start` rolls at 18:00 and holds local midnight across both DST
   transitions.
2. Partial first and last months are excluded from every rate.
3. A constructed month with its low on session 0 and high on the final session asserts
   Early/Late and the quintile edges.
4. Months of different lengths (19 vs 23 sessions) map to the same normalized position
   for the same relative placement.
5. Week-of-month output carries `months_present` and `avg_sessions`, and W5 reports
   fewer of both than W1–W4.
6. A touch at 18:05 on the month's first session does **not** count as a monthly-open
   retap; a touch at 09:31 does.
7. A month whose level is touched only on session 10 produces the expected forward-touch
   decile curve.
8. The month-state conditioner allow-list contains no whole-month label.
9. Monthly targets appear only for `Third_In_Month == Early` rows and carry `n_months`.
10. CLI exposes the three new modules and routes each to its own symbol-scoped directory.

## Documentation

- README gains modules 8–10 and the month conventions.
- AGENTS.md gains the session-month convention and two Research Hazards entries: the
  knowability rule for conditioners, and the session-position scope rule for monthly
  targets.

## Out of Scope

- **No new tracked artifacts in `output/examples/`.** The README results are pinned to
  the 2026-05-11 vintage until a full regeneration pass; adding monthly charts now would
  mix two vintages in one gallery. Monthly outputs stay local.
- **No train/OOS split for monthly outputs**, per the sampling reality above.
- **No three-phase PO3 structure** (accumulation / manipulation / distribution).
- **No changes to the weekly or intraday results.** Component 4 adds columns to existing
  row builders and adds new summary files; it does not alter any existing figure.
