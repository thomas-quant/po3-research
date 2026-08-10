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
