import numpy as np
import pandas as pd
import pytest

import analysis

ET = "America/New_York"


def _weekday_sessions_in_month(year: int, month: int, count: int) -> list:
    """First `count` weekday session dates in one calendar month."""
    out = []
    day = pd.Timestamp(year=year, month=month, day=1)
    while day.month == month and len(out) < count:
        if day.dayofweek < 5:
            out.append(day)
        day += pd.Timedelta(days=1)
    return out


def _hourly_frame(session_dates, prices=None) -> pd.DataFrame:
    """
    Hourly ET bars for each session date, prior evening 18:00 through 16:00.

    Timestamps are built naive and localized once, so the frame stays correct
    across both DST transitions. Every bar of a session carries the same price,
    so `prices[k]` decides which session holds the monthly high and low.
    """
    prices = list(prices) if prices is not None else [100.0] * len(session_dates)
    stamps, values = [], []
    for session, price in zip(session_dates, prices):
        block = pd.date_range(
            pd.Timestamp(session).normalize() - pd.Timedelta(hours=6), periods=23, freq="1h")
        stamps.extend(block)
        values.extend([price] * 23)
    index = pd.DatetimeIndex(stamps).tz_localize(ET, ambiguous=True, nonexistent="shift_forward")
    v = np.array(values, dtype=float)
    return pd.DataFrame(
        {"Open": v, "High": v + 1.0, "Low": v - 1.0, "Close": v, "Volume": 10.0}, index=index)


def _minute_frame(bars) -> pd.DataFrame:
    """1-minute frame from explicit (naive timestamp, open, high, low, close) tuples."""
    index = pd.DatetimeIndex([pd.Timestamp(b[0]) for b in bars]).tz_localize(
        ET, ambiguous=True, nonexistent="shift_forward")
    frame = pd.DataFrame(
        {
            "Open": [b[1] for b in bars],
            "High": [b[2] for b in bars],
            "Low": [b[3] for b in bars],
            "Close": [b[4] for b in bars],
            "Volume": [10.0] * len(bars),
        },
        index=index,
    )
    return frame.sort_index()


def _session_bars(session, price: float) -> list:
    """The 18:00 prior-evening print, an 09:31 bar, and the 16:00 close, all at `price`."""
    day = pd.Timestamp(session).normalize()
    return [
        (day - pd.Timedelta(hours=6), price, price + 1, price - 1, price),
        (day + pd.Timedelta(hours=9, minutes=31), price, price + 1, price - 1, price),
        (day + pd.Timedelta(hours=16), price, price + 1, price - 1, price),
    ]


@pytest.mark.parametrize("evening,expected_month_start", [
    ("2025-07-31 18:00", "2025-08-01"),   # the Globex print that opens August
    ("2026-03-15 19:00", "2026-03-01"),   # month start sits before the spring transition
    ("2026-11-15 19:00", "2026-11-01"),   # month start sits after the autumn transition
])
def test_trading_month_start_rolls_at_18_00_and_holds_local_midnight_across_dst(evening, expected_month_start):
    ts = pd.Timestamp(evening, tz=ET)

    start = analysis.trading_month_start(ts)

    assert start == pd.Timestamp(expected_month_start, tz=ET)
    assert start.hour == 0
    assert analysis.trading_month_start_index(pd.DatetimeIndex([ts]))[0] == start


def test_week_of_month_buckets_calendar_days_into_w1_to_w5():
    assert analysis.week_of_month(pd.Timestamp("2026-03-01")) == "W1"
    assert analysis.week_of_month(pd.Timestamp("2026-03-07")) == "W1"
    assert analysis.week_of_month(pd.Timestamp("2026-03-08")) == "W2"
    assert analysis.week_of_month(pd.Timestamp("2026-03-29")) == "W5"


def test_month_with_low_on_session_zero_and_high_on_the_last_session_maps_to_early_and_late():
    sessions = _weekday_sessions_in_month(2026, 3, 22)
    prices = [90.0] + [100.0] * 20 + [110.0]

    monthly = analysis.build_monthly(_hourly_frame(sessions, prices))

    row = monthly.iloc[0]
    assert row["N_Sessions"] == 22
    assert row["Low_Session_Index"] == 0
    assert row["High_Session_Index"] == 21
    assert row["Low_Third"] == "Early"
    assert row["High_Third"] == "Late"
    assert row["Low_Quintile"] == "Q1"
    assert row["High_Quintile"] == "Q5"
    assert row["Low_Session_Pos"] == 0.0
    assert row["High_Session_Pos"] == 1.0


def test_months_of_different_lengths_share_one_normalized_position():
    july = _weekday_sessions_in_month(2026, 7, 23)
    august = _weekday_sessions_in_month(2026, 8, 19)
    assert (len(july), len(august)) == (23, 19)
    july_prices = [100.0] * 23
    july_prices[11] = 120.0                      # 11 / 22 == 0.5
    august_prices = [100.0] * 19
    august_prices[9] = 120.0                     # 9 / 18 == 0.5
    frame = pd.concat([
        _hourly_frame(july, july_prices), _hourly_frame(august, august_prices)]).sort_index()

    monthly = analysis.build_monthly(frame).set_index("Month_Of_Year")

    assert monthly.loc[7, "High_Session_Index"] == 11
    assert monthly.loc[8, "High_Session_Index"] == 9
    assert monthly.loc[7, "High_Session_Pos"] == monthly.loc[8, "High_Session_Pos"] == 0.5


def test_partial_first_and_last_months_are_flagged_and_dropped():
    """The rate side of this — that every share uses the complete months as its
    denominator — is asserted in test_monthly_share_table_reports_n_ci_and_a_sparse_flag_per_bucket."""
    months = [
        _weekday_sessions_in_month(2026, 1, 21),
        _weekday_sessions_in_month(2026, 2, 20),
        _weekday_sessions_in_month(2026, 3, 22),
        _weekday_sessions_in_month(2026, 4, 22),
    ]
    frame = pd.concat([_hourly_frame(m) for m in months]).sort_index()

    monthly = analysis.build_monthly(frame)
    complete = analysis.complete_months(monthly)

    assert list(monthly["Month_Of_Year"]) == [1, 2, 3, 4]
    assert list(monthly["Is_Partial"]) == [True, False, False, True]
    assert list(complete["Month_Of_Year"]) == [2, 3]


def test_monthly_rows_carry_prior_month_direction():
    january = _weekday_sessions_in_month(2026, 1, 21)
    february = _weekday_sessions_in_month(2026, 2, 20)
    frame = pd.concat([
        _hourly_frame(january, [100.0 + i for i in range(21)]),   # bullish
        _hourly_frame(february, [100.0 - i for i in range(20)]),  # bearish
    ]).sort_index()

    monthly = analysis.build_monthly(frame)

    assert list(monthly["Bull_Bear"]) == ["Bullish", "Bearish"]
    assert pd.isna(monthly.iloc[0]["Prev_Bull_Bear"])
    assert monthly.iloc[1]["Prev_Bull_Bear"] == "Bullish"


def _four_month_frame() -> pd.DataFrame:
    """Jan and Apr partial, Feb and Mar complete. Feb 2026 has no W5; Mar 2026 has two."""
    months = [
        _weekday_sessions_in_month(2026, 1, 21),
        _weekday_sessions_in_month(2026, 2, 20),
        _weekday_sessions_in_month(2026, 3, 22),
        _weekday_sessions_in_month(2026, 4, 22),
    ]
    return pd.concat([_hourly_frame(m) for m in months]).sort_index()


def test_monthly_share_table_reports_n_ci_and_a_sparse_flag_per_bucket():
    frame = _four_month_frame()
    monthly = analysis.build_monthly(frame)

    table = analysis.monthly_share_table(monthly, "High_Third", order=analysis.MONTH_THIRDS)

    overall = table[table["Direction_Scope"].eq("All")]
    assert list(overall["High_Third"]) == ["Early", "Mid", "Late"]
    assert set(overall["months"]) == {2}                 # only the complete months
    assert set(table["Direction_Scope"]) == {"All", "Bullish", "Bearish"}
    for column in ["n", "pct", "ci_low", "ci_high", "is_sparse"]:
        assert column in table
    assert overall["pct"].sum() == pytest.approx(100.0)
    assert bool(overall["is_sparse"].all())              # 2 months is far under SPARSE_MONTHS


def test_week_of_month_table_reports_exposure_and_w5_spans_less_of_the_month():
    frame = _four_month_frame()
    monthly = analysis.build_monthly(frame)

    exposure = analysis.month_week_exposure(frame, monthly)
    tables = analysis.monthly_extreme_timing_tables(frame, monthly)
    table = tables["high_timing_by_week_of_month"]

    exposure = exposure.set_index("Week_Of_Month")
    assert exposure.loc["W5", "months_present"] == 1     # February 2026 has no W5 at all
    assert exposure.loc["W1", "months_present"] == 2
    assert exposure.loc["W5", "avg_sessions"] < exposure.loc["W1", "avg_sessions"]

    assert "months_present" in table
    assert "avg_sessions" in table
    w5 = table[table["High_Week_Of_Month"].eq("W5") & table["Direction_Scope"].eq("All")].iloc[0]
    w1 = table[table["High_Week_Of_Month"].eq("W1") & table["Direction_Scope"].eq("All")].iloc[0]
    assert w5["months_present"] < w1["months_present"]
    assert w5["avg_sessions"] < w1["avg_sessions"]


def test_every_extreme_timing_cut_is_produced_for_high_and_low():
    frame = _four_month_frame()
    monthly = analysis.build_monthly(frame)

    tables = analysis.monthly_extreme_timing_tables(frame, monthly)

    assert set(tables) == {
        f"{event}_timing_by_{cut}"
        for event in ["high", "low"]
        for cut in ["third", "quintile", "week_of_month", "weekday", "session", "day_of_month"]
    }


def test_monthly_extremes_runner_writes_every_table_and_the_primary_charts(tmp_path):
    frame = _four_month_frame()

    monthly = analysis.run_monthly_extremes_research(frame, symbol="ES", output_dir=tmp_path)

    assert not monthly.empty
    written = {p.name for p in tmp_path.iterdir()}
    assert "monthly_rows.csv" in written
    for event in ["high", "low"]:
        for cut in ["third", "quintile", "week_of_month", "weekday", "session", "day_of_month"]:
            assert f"{event}_timing_by_{cut}.csv" in written
        for cut in ["third", "quintile", "week_of_month"]:
            assert f"{event}_timing_by_{cut}.png" in written
    # Descriptive only: no train/OOS column anywhere in the monthly outputs.
    assert "Split" not in pd.read_csv(tmp_path / "monthly_rows.csv").columns
    assert "Split" not in pd.read_csv(tmp_path / "high_timing_by_third.csv").columns


def _three_month_minute_bars(march_first_session_930_low: float = 299.0) -> list:
    """
    Feb / Mar / Apr at 100 / 300 / 400, with March opening at 200 on the evening print.

    The 18:05 bar retaps that 200 open in the thin hours. `march_first_session_930_low`
    decides whether the 09:31 bar on the first March session also reaches it.
    """
    february = _weekday_sessions_in_month(2026, 2, 20)
    march = _weekday_sessions_in_month(2026, 3, 22)
    april = _weekday_sessions_in_month(2026, 4, 22)
    bars = []
    for session in february:
        bars += _session_bars(session, 100.0)
    evening = pd.Timestamp(march[0]).normalize() - pd.Timedelta(hours=6)
    bars += [
        (evening, 200.0, 201.0, 199.0, 200.5),
        (evening + pd.Timedelta(minutes=5), 200.5, 201.0, 199.5, 200.2),
    ]
    for position, session in enumerate(march):
        day = pd.Timestamp(session).normalize()
        low = march_first_session_930_low if position == 0 else 299.0
        bars += [
            (day + pd.Timedelta(hours=9, minutes=31), 300.0, 301.0, low, 300.0),
            (day + pd.Timedelta(hours=16), 300.0, 301.0, 299.0, 300.0),
        ]
    for session in april:
        bars += _session_bars(session, 400.0)
    return bars


def test_monthly_open_retap_ignores_the_evening_print_and_starts_at_09_30():
    thin_only = analysis.build_monthly_level_rows(_minute_frame(_three_month_minute_bars()))
    with_rth = analysis.build_monthly_level_rows(
        _minute_frame(_three_month_minute_bars(march_first_session_930_low=199.0)))

    def march_open(rows):
        march = rows[rows["Month_Start"].dt.month.eq(3) & rows["Level_Name"].eq("Monthly_Open")]
        return march.iloc[0]

    # 18:05 is inside the thin hours that set the 200 open: not a retap.
    assert bool(march_open(thin_only)["Touched"]) is False
    assert march_open(thin_only)["Touch_Bars_Total"] == 0
    assert pd.isna(march_open(thin_only)["First_Touch_Timestamp"])

    # 09:31 on the month's first RTH session is.
    touched = march_open(with_rth)
    assert bool(touched["Touched"]) is True
    assert touched["Touch_Bars_Total"] == 1
    assert touched["First_Touch_Timestamp"].hour == 9
    assert touched["First_Touch_Session_Index"] == 0
    assert touched["First_Touch_Third"] == "Early"


def test_monthly_level_rows_cover_every_level_and_skip_the_undefined_prior_month():
    rows = analysis.build_monthly_level_rows(_minute_frame(_three_month_minute_bars()))

    february = rows[rows["Month_Start"].dt.month.eq(2)]
    march = rows[rows["Month_Start"].dt.month.eq(3)]
    # February is the first month in the sample; it has no prior month to reference.
    assert list(february["Level_Name"]) == ["Monthly_Open"]
    assert list(march["Level_Name"]) == analysis.MONTHLY_LEVELS
    assert march.set_index("Level_Name").loc["Prior_Month_Close", "Level_Value"] == 100.0
    assert march.set_index("Level_Name").loc["Prior_Month_High", "Level_Value"] == 101.0
    assert bool(march["Is_Partial"].eq(False).all())
    assert bool(february["Is_Partial"].eq(True).all())


def test_monthly_level_touch_distribution_and_first_touch_third_use_complete_months():
    rows = analysis.build_monthly_level_rows(
        _minute_frame(_three_month_minute_bars(march_first_session_930_low=199.0)))

    distribution = analysis.monthly_level_touch_distribution(rows)
    thirds = analysis.monthly_level_first_touch_by_third(rows)

    assert set(distribution["Level_Name"]) <= set(analysis.MONTHLY_LEVELS)
    assert set(distribution["months"]) == {1}          # only March is complete
    for column in ["n", "pct", "ci_low", "ci_high", "is_sparse"]:
        assert column in distribution
        assert column in thirds
    assert list(thirds[thirds["Level_Name"].eq("Monthly_Open")]["First_Touch_Third"]) == analysis.MONTH_THIRDS


def _touched_on_session_ten_frame() -> pd.DataFrame:
    """
    Jan at 100, a complete 20-session March at 300, May at 500.

    Only session 10 of March dips back to 100, so the prior-month close is touched
    from session 0 through session 10 and from nowhere after it.
    """
    january = _weekday_sessions_in_month(2026, 1, 21)
    march = _weekday_sessions_in_month(2026, 3, 20)
    may = _weekday_sessions_in_month(2026, 5, 20)
    bars = []
    for session in january:
        bars += _session_bars(session, 100.0)
    for position, session in enumerate(march):
        day = pd.Timestamp(session).normalize()
        low = 100.0 if position == 10 else 299.0
        bars += [
            (day - pd.Timedelta(hours=6), 300.0, 301.0, 299.0, 300.0),
            (day + pd.Timedelta(hours=9, minutes=31), 300.0, 301.0, low, 300.0),
            (day + pd.Timedelta(hours=16), 300.0, 301.0, 299.0, 300.0),
        ]
    for session in may:
        bars += _session_bars(session, 500.0)
    return _minute_frame(bars)


def test_forward_touch_decile_curve_for_a_level_touched_only_on_session_ten():
    by_index, by_decile = analysis.monthly_level_forward_touch(_touched_on_session_ten_frame())

    decile = by_decile[by_decile["Level_Name"].eq("Prior_Month_Close")].set_index("Decile")
    # 20 sessions, so decile d starts at session int(d * 20 / 10) = 0, 2, 4 ... 18.
    # D6 starts at session 10 — the last start from which the touch is still ahead.
    assert list(decile.loc[[f"D{d}" for d in range(1, 7)], "touch_pct"]) == [100.0] * 6
    assert list(decile.loc[[f"D{d}" for d in range(7, 11)], "touch_pct"]) == [0.0] * 4
    assert set(decile["n_months"]) == {1}      # every month contributes to every decile

    index = by_index[by_index["Level_Name"].eq("Prior_Month_Close")].set_index("Session_Index")
    assert index.loc[10, "touch_pct"] == 100.0
    assert index.loc[11, "touch_pct"] == 0.0
    for column in ["n_months", "touch_months", "ci_low", "ci_high", "is_sparse"]:
        assert column in by_index
        assert column in by_decile


def test_forward_touch_skips_partial_months():
    by_index, _by_decile = analysis.monthly_level_forward_touch(_touched_on_session_ten_frame())

    # January and May are the sample edges; only March contributes.
    assert set(by_index["n_months"]) == {1}
    assert by_index["Session_Index"].max() == 19


def test_monthly_levels_runner_writes_every_output(tmp_path):
    frame = _touched_on_session_ten_frame()
    source = tmp_path / "bars.parquet"
    frame.reset_index(names="datetime_utc").assign(
        datetime_utc=lambda d: d["datetime_utc"].dt.tz_convert("UTC")).to_parquet(source)

    rows = analysis.run_monthly_levels_research(
        symbol="ES", path=str(source), output_dir=tmp_path / "out")

    assert not rows.empty
    written = {p.name for p in (tmp_path / "out").iterdir()}
    assert written == {
        "monthly_level_rows.csv",
        "monthly_level_touch_distribution.csv",
        "monthly_level_first_touch_by_third.csv",
        "monthly_level_forward_touch_by_session_index.csv",
        "monthly_level_forward_touch_by_decile.csv",
    }


def test_month_state_conditioners_contain_no_whole_month_label():
    monthly = analysis.build_monthly(_four_month_frame())

    for conditioner in analysis.MONTH_STATE_CONDITIONERS:
        assert not analysis.is_whole_month_label(conditioner), conditioner

    # The guard has teeth: every column build_monthly derives from the finished
    # month is caught, and none of them leaked into the allow-list.
    for label in ["Bull_Bear", "Monthly_Close", "Monthly_High", "Monthly_Low",
                  "High_Third", "Low_Third", "High_Session_Index", "Low_Weekday"]:
        assert label in monthly.columns
        assert analysis.is_whole_month_label(label), label
    assert not set(analysis.MONTH_STATE_CONDITIONERS) & set(analysis.WHOLE_MONTH_LABELS)


def test_month_state_by_session_is_knowable_and_carries_a_lagged_return():
    january = _weekday_sessions_in_month(2026, 1, 21)
    february = _weekday_sessions_in_month(2026, 2, 20)
    frame = pd.concat([
        _hourly_frame(january, [100.0 + i for i in range(21)]),
        _hourly_frame(february, [100.0 + i for i in range(20)]),
    ]).sort_index()
    monthly = analysis.build_monthly(frame)

    state = analysis.build_month_state_by_session(frame, monthly)

    january_state = state[state["Month_Of_Year"].eq(1)].reset_index(drop=True)
    assert list(january_state["Session_Index_In_Month"]) == list(range(21))
    assert set(january_state["N_Sessions"]) == {21}
    assert january_state.loc[0, "Third_In_Month"] == "Early"
    assert january_state.loc[20, "Third_In_Month"] == "Late"
    assert january_state.loc[0, "Session_Pos_In_Month"] == 0.0
    assert january_state.loc[20, "Session_Pos_In_Month"] == 1.0
    assert january_state.loc[1, "Month_Return_So_Far_Pct"] == pytest.approx(1.0)
    # The lagged twin is knowable at the session open, so it can condition a target
    # that includes the same session's close.
    assert pd.isna(january_state.loc[0, "Month_Return_To_Prior_Close_Pct"])
    assert january_state.loc[2, "Month_Return_To_Prior_Close_Pct"] == pytest.approx(1.0)
    assert pd.isna(january_state.loc[0, "Prior_Month_Bull_Bear"])
    assert set(state[state["Month_Of_Year"].eq(2)]["Prior_Month_Bull_Bear"]) == {"Bullish"}
    for column in analysis.MONTH_STATE_CONDITIONERS:
        assert column in state


def test_weeks_spanning_two_session_months_are_flagged():
    march = _weekday_sessions_in_month(2026, 3, 22)
    april = _weekday_sessions_in_month(2026, 4, 22)
    frame = pd.concat([_hourly_frame(march), _hourly_frame(april)]).sort_index()
    monthly = analysis.build_monthly(frame)

    week_state = analysis.build_week_month_state(
        analysis.build_month_state_by_session(frame, monthly)).set_index("Week_Start")

    straddling = pd.Timestamp("2026-03-30", tz=ET)   # Mar 30-31 then Apr 1-3
    assert bool(week_state.loc[straddling, "Straddles_Month_Boundary"])
    assert week_state.loc[straddling, "Month_Start"] == pd.Timestamp("2026-03-01", tz=ET)
    assert not bool(week_state.loc[pd.Timestamp("2026-03-16", tz=ET), "Straddles_Month_Boundary"])


def _month_state_rows() -> pd.DataFrame:
    """
    Day-level rows for Jan–Apr 2020 with month-state and monthly targets attached.

    2020 sits before `TRAIN_END`, so every row is a Train row and the p25/p75
    bucketing actually has quantiles to learn from.
    """
    months = [_weekday_sessions_in_month(2020, month, 19) for month in (1, 2, 3, 4)]
    frame = pd.concat([
        _hourly_frame(m, [100.0 + i for i in range(len(m))]) for m in months]).sort_index()
    monthly = analysis.build_monthly(frame)
    state = analysis.build_month_state_by_session(frame, monthly)
    sessions = pd.DatetimeIndex(pd.unique(analysis.intraday_trading_day_index(frame.index)))
    rows = pd.DataFrame({
        "Trading_Day": sessions,
        "Split": np.where(sessions <= analysis.TRAIN_END, "Train", "OOS"),
        "Level_Name": "Globex_Open",
        "Day_Bullish": [bool(i % 2) for i in range(len(sessions))],
    })
    return analysis.attach_monthly_targets(
        analysis.attach_month_state(rows, state), monthly)


def test_monthly_targets_are_scored_on_early_month_rows_only_and_carry_n_months():
    rows = _month_state_rows()

    summary = analysis.month_state_outcomes(
        rows, ["Day_Bullish"], group_cols=["Split", "Level_Name"])

    monthly_rows = summary[summary["Target"].isin(analysis.MONTHLY_TARGETS)]
    assert not monthly_rows.empty
    assert set(monthly_rows["Month_Scope"]) == {"Early"}
    assert bool((monthly_rows["n_months"] > 0).all())
    assert bool((monthly_rows["n_months"] <= monthly_rows["n"]).all())

    intraday_rows = summary[summary["Target"].eq("Day_Bullish")]
    assert set(intraday_rows["Month_Scope"]) == {"All"}
    assert {"Split", "Level_Name", "Conditioner", "Conditioner_Value", "pct"} <= set(summary.columns)


def test_month_state_summary_covers_every_allowed_conditioner_and_no_other():
    rows = _month_state_rows()

    summary = analysis.month_state_outcomes(rows, ["Day_Bullish"])

    expected = {
        f"{c}_Bucket" if c in analysis.MONTH_STATE_NUMERIC_CONDITIONERS else c
        for c in analysis.MONTH_STATE_CONDITIONERS
    }
    assert set(summary["Conditioner"]) == expected
    assert not set(summary["Conditioner"]) & set(analysis.WHOLE_MONTH_LABELS)


def test_numeric_month_state_conditioners_bucket_on_train_quantiles():
    rows = _month_state_rows()

    bucketed = analysis.apply_month_state_buckets(rows)

    assert set(rows["Split"]) == {"Train"}
    assert set(bucketed["Month_Return_So_Far_Pct_Bucket"].dropna()) == {
        "P25 Low", "Middle 25-75%", "P75 High"}
    assert set(bucketed["Session_Pos_In_Month_Bucket"].dropna()) == {
        "P25 Low", "Middle 25-75%", "P75 High"}
    # The lagged twin has no value on each month's first session.
    assert bucketed["Month_Return_To_Prior_Close_Pct_Bucket"].isna().sum() == 4
