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
