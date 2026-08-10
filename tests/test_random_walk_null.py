import numpy as np
import pandas as pd
import pytest

import analysis

ET = "America/New_York"


def _walk_frame(n_periods: int, sessions_per_period: int, bars_per_session: int,
                drift: float = 0.0, sigma: float = 0.01, seed: int = 3) -> pd.DataFrame:
    """
    A synthetic hourly frame laid out as consecutive weekday sessions.

    The price path is a random walk with `drift` per bar, so the null machinery can
    be exercised without depending on the real ES sample.
    """
    rng = np.random.default_rng(seed)
    total_sessions = n_periods * sessions_per_period
    stamps, prices = [], []
    day = pd.Timestamp("2021-01-04")
    log_price = np.log(100.0)
    for _ in range(total_sessions):
        while day.dayofweek >= 5:
            day += pd.Timedelta(days=1)
        start = day.normalize() - pd.Timedelta(hours=6)
        for bar in range(bars_per_session):
            log_price += drift + rng.normal(0.0, sigma)
            stamps.append(start + pd.Timedelta(hours=bar))
            prices.append(np.exp(log_price))
        day += pd.Timedelta(days=1)
    index = pd.DatetimeIndex(stamps).tz_localize(ET, ambiguous=True, nonexistent="shift_forward")
    v = np.asarray(prices, dtype=float)
    return pd.DataFrame(
        {"Open": v, "High": v * 1.0005, "Low": v * 0.9995, "Close": v, "Volume": 10.0},
        index=index).sort_index()


def _rising_frame(n_sessions: int = 40, bars_per_session: int = 6) -> pd.DataFrame:
    """Every bar closes above the last, so every session return is positive."""
    stamps, prices = [], []
    day = pd.Timestamp("2021-01-04")
    price = 100.0
    for _ in range(n_sessions):
        while day.dayofweek >= 5:
            day += pd.Timedelta(days=1)
        start = day.normalize() - pd.Timedelta(hours=6)
        for bar in range(bars_per_session):
            price *= 1.001
            stamps.append(start + pd.Timedelta(hours=bar))
            prices.append(price)
        day += pd.Timedelta(days=1)
    index = pd.DatetimeIndex(stamps).tz_localize(ET, ambiguous=True, nonexistent="shift_forward")
    v = np.asarray(prices, dtype=float)
    return pd.DataFrame(
        {"Open": v, "High": v, "Low": v, "Close": v, "Volume": 10.0}, index=index).sort_index()


def _period_and_position(df: pd.DataFrame, period: str):
    return (
        analysis.PERIOD_KEY_FUNCS[period](df.index),
        analysis.intraday_trading_day_index(df.index),
    )


def test_arcsine_null_shares_matches_the_closed_form_and_is_u_shaped():
    thirds = analysis.arcsine_null_shares(3)
    quintiles = analysis.arcsine_null_shares(5)

    # F(t) = 2/pi * arcsin(sqrt t), differenced across equal-width buckets.
    assert thirds == pytest.approx([39.1826, 21.6347, 39.1826], abs=1e-3)
    assert quintiles == pytest.approx([29.5167, 14.0739, 12.8188, 14.0739, 29.5167], abs=1e-3)
    assert sum(thirds) == pytest.approx(100.0)
    assert sum(quintiles) == pytest.approx(100.0)
    # The ends carry more mass than the middle: a U, not a uniform 33.3 / 20.0.
    assert thirds[0] > thirds[1] < thirds[2]
    assert quintiles[0] > max(quintiles[1:4])


def test_driftless_random_walk_control_reproduces_the_arcsine_law():
    """The control is only trustworthy if it recovers the closed form it should."""
    df = _walk_frame(n_periods=40, sessions_per_period=10, bars_per_session=8)
    period_index, position_index = _period_and_position(df, "month")

    table = analysis.extreme_position_null(
        df, period_index, position_index, n_buckets=3,
        controls=["arcsine", "driftless"], n_sim=400, seed=5)

    driftless = table[table["Null"].eq("driftless") & table["Event"].eq("High")]
    arcsine = table[table["Null"].eq("arcsine") & table["Event"].eq("High")]
    assert list(driftless["Bucket"]) == analysis.MONTH_THIRDS
    assert driftless["null_pct"].to_numpy() == pytest.approx(
        arcsine["null_pct"].to_numpy(), abs=4.0)
    assert driftless["null_pct"].sum() == pytest.approx(100.0, abs=0.01)


def test_drift_control_pushes_the_high_late_and_the_low_early():
    df = _walk_frame(n_periods=40, sessions_per_period=10, bars_per_session=8,
                     drift=0.004, sigma=0.01)
    period_index, position_index = _period_and_position(df, "month")

    table = analysis.extreme_position_null(
        df, period_index, position_index, n_buckets=3,
        controls=["driftless", "drift"], n_sim=400, seed=5)

    def cell(null, event, bucket):
        row = table[table["Null"].eq(null) & table["Event"].eq(event) & table["Bucket"].eq(bucket)]
        return float(row["null_pct"].iloc[0])

    assert cell("drift", "High", "Late") > cell("driftless", "High", "Late")
    assert cell("drift", "Low", "Early") > cell("driftless", "Low", "Early")
    # Drift tilts the U; it does not fill in the middle.
    assert cell("drift", "High", "Mid") < cell("drift", "High", "Late")


def test_shuffle_control_holds_each_period_realized_return_fixed():
    """
    Every session return is positive, so no reordering can move the extremes:
    the high is the last session and the low the first, in every shuffle.
    """
    df = _rising_frame()
    period_index, position_index = _period_and_position(df, "week")

    table = analysis.extreme_position_null(
        df, period_index, position_index, n_buckets=3,
        controls=["shuffle"], n_sim=100, seed=5)

    high_late = table[table["Null"].eq("shuffle") & table["Event"].eq("High")
                      & table["Bucket"].eq("Late")]
    low_early = table[table["Null"].eq("shuffle") & table["Event"].eq("Low")
                      & table["Bucket"].eq("Early")]
    assert float(high_late["null_pct"].iloc[0]) == pytest.approx(100.0)
    assert float(low_early["null_pct"].iloc[0]) == pytest.approx(100.0)
    assert float(high_late["observed_pct"].iloc[0]) == pytest.approx(100.0)


def test_the_same_call_serves_weekly_and_monthly_periods():
    """Timeframe-agnostic: only the period key changes."""
    df = _walk_frame(n_periods=30, sessions_per_period=10, bars_per_session=8)

    tables = {}
    for period in ["week", "month"]:
        period_index, position_index = _period_and_position(df, period)
        tables[period] = analysis.extreme_position_null(
            df, period_index, position_index, n_buckets=5,
            controls=["arcsine", "drift"], n_sim=200, seed=5)

    for period, table in tables.items():
        assert list(table[table["Null"].eq("arcsine") & table["Event"].eq("High")]["Bucket"]) \
            == analysis.MONTH_QUINTILES, period
        assert table["observed_pct"].notna().all(), period
    # A week holds ~5 sessions and a month ~10 here, so the period counts differ.
    assert tables["week"]["n_periods"].iloc[0] > tables["month"]["n_periods"].iloc[0]


def test_null_table_schema_is_stable_and_the_run_is_reproducible():
    df = _walk_frame(n_periods=25, sessions_per_period=8, bars_per_session=6, drift=0.001)
    period_index, position_index = _period_and_position(df, "month")

    kwargs = dict(n_buckets=3, controls=analysis.RW_NULL_CONTROLS, n_sim=150, seed=9)
    table = analysis.extreme_position_null(df, period_index, position_index, **kwargs)
    again = analysis.extreme_position_null(df, period_index, position_index, **kwargs)

    assert set(table.columns) == {
        "Event", "Bucket", "n_periods", "observed_pct", "Null", "null_pct",
        "ci_low", "ci_high", "p", "is_sparse"}
    assert set(table["Null"]) == set(analysis.RW_NULL_CONTROLS)
    assert set(table["Event"]) == {"High", "Low"}
    pd.testing.assert_frame_equal(table, again)

    # The arcsine rung is closed form, so it carries no band and no p-value.
    arcsine = table[table["Null"].eq("arcsine")]
    assert arcsine["ci_low"].isna().all()
    assert arcsine["p"].isna().all()
    simulated = table[~table["Null"].eq("arcsine")]
    assert bool(simulated["p"].between(0.0, 1.0).all())
    assert bool((simulated["ci_low"] <= simulated["null_pct"]).all())
    assert bool((simulated["null_pct"] <= simulated["ci_high"]).all())
    for event in ["High", "Low"]:
        observed = table[table["Event"].eq(event) & table["Null"].eq("arcsine")]["observed_pct"]
        assert observed.sum() == pytest.approx(100.0)
