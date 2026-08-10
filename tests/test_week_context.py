import numpy as np
import pandas as pd
import pytest

import analysis

ET = "America/New_York"


def _minute_session(session_date, prices) -> list:
    """
    One session's worth of 1-minute bars laid out across all four relative windows.

    `prices` maps a window name to the constant price used for its bars, so a test can
    place a move in one window and leave the others flat. Bars are sparse — a handful
    per window — because every builder here works off window slices, not bar counts.
    """
    day = pd.Timestamp(session_date).normalize()
    layout = {
        "Globex_to_Midnight": [day - pd.Timedelta(hours=6) + pd.Timedelta(minutes=m)
                               for m in (0, 60, 120)],
        "Midnight_to_0930": [day + pd.Timedelta(minutes=m) for m in (0, 120, 480)],
        "0930_to_1300": [day + pd.Timedelta(hours=9, minutes=30) + pd.Timedelta(minutes=m)
                         for m in (0, 60, 120)],
        "1300_to_Close": [day + pd.Timedelta(hours=13) + pd.Timedelta(minutes=m)
                          for m in (0, 60, 180)],
    }
    bars = []
    for window_name, stamps in layout.items():
        price = prices[window_name]
        for stamp in stamps:
            bars.append((stamp, price, price + 1.0, price - 1.0, price))
    return bars


def _minute_frame(bars) -> pd.DataFrame:
    index = pd.DatetimeIndex([b[0] for b in bars]).tz_localize(
        ET, ambiguous=True, nonexistent="shift_forward")
    return pd.DataFrame(
        {
            "Open": [b[1] for b in bars],
            "High": [b[2] for b in bars],
            "Low": [b[3] for b in bars],
            "Close": [b[4] for b in bars],
            "Volume": [10.0] * len(bars),
        },
        index=index,
    ).sort_index()


def _week_frame(n_weeks: int = 3, flat: float = 100.0) -> pd.DataFrame:
    """Consecutive weekday sessions, every window flat, so state is easy to reason about."""
    bars = []
    day = pd.Timestamp("2022-03-07")
    for _ in range(n_weeks * 5):
        while day.dayofweek >= 5:
            day += pd.Timedelta(days=1)
        bars.extend(_minute_session(day, {w: flat for w in analysis.RELATIVE_WINDOWS}))
        day += pd.Timedelta(days=1)
    return _minute_frame(bars)


# ── The knowability rule ───────────────────────────────────────────────────────


def test_no_window_outcome_label_is_a_conditioner():
    """
    The mirror of the month-state allow-list guard: a conditioner may never be a
    column describing the window it is scoring, or anything after it.
    """
    conditioners = analysis.WEEK_STATE_CONDITIONERS + analysis.DAY_PRIOR_CONDITIONERS
    for name in conditioners:
        assert not analysis.is_window_outcome_label(name), name
    for label in analysis.WINDOW_OUTCOME_LABELS:
        assert label not in conditioners, label


def test_week_state_is_lagged_to_the_prior_session_close():
    """
    Week state must stop at the previous session. A Monday has no prior session in its
    week, so every path conditioner is NaN there rather than borrowing Friday's.
    """
    frame = _week_frame()
    state = analysis.build_week_state_by_session(frame)

    mondays = state[state["Session_Index_In_Week"].eq(0)]
    assert not mondays.empty
    assert mondays["Week_Return_To_Prior_Close_Pct"].isna().all()
    assert mondays["Week_Range_So_Far_Pct"].isna().all()
    assert mondays["Above_Weekly_Open_At_Prior_Close"].isna().all()
    # Later sessions in the week do carry it.
    assert state[state["Session_Index_In_Week"].eq(2)]["Week_Range_So_Far_Pct"].notna().any()


def test_week_range_so_far_excludes_the_current_session():
    """A spike today must not appear in today's own `Week_Range_So_Far_Pct`."""
    bars = []
    day = pd.Timestamp("2022-03-07")
    for offset in range(3):
        prices = {w: 100.0 for w in analysis.RELATIVE_WINDOWS}
        if offset == 2:
            prices["0930_to_1300"] = 200.0          # the spike, on the third session
        bars.extend(_minute_session(day + pd.Timedelta(days=offset), prices))
    state = analysis.build_week_state_by_session(_minute_frame(bars))

    spike_row = state.iloc[2]
    # Sessions 0 and 1 were flat, so the range known before the spike session is ~0.
    assert spike_row["Week_Range_So_Far_Pct"] == pytest.approx(2.0, abs=0.5)


# ── Day-prior state and window rows ────────────────────────────────────────────


def test_day_prior_state_is_empty_for_the_first_window_of_the_session():
    """
    `Globex_to_Midnight` opens the session, so there is no prior intraday path to
    condition it on. That must be NaN, not zero — zero would pool it with genuinely
    flat sessions in the middle bucket.
    """
    rows = analysis.build_intraday_window_rows(_week_frame())

    first = rows[rows["Window_Name"].eq("Globex_to_Midnight")]
    assert first["Day_Return_To_Window_Open_Pct"].isna().all()
    assert first["Day_Range_To_Window_Open_Pct"].isna().all()
    assert first["Prior_Window_Return_Pct"].isna().all()
    later = rows[rows["Window_Name"].eq("1300_to_Close")]
    assert later["Day_Return_To_Window_Open_Pct"].notna().any()


def test_window_range_ratio_divides_by_the_prior_session_same_window():
    """The vol-clustering denominator is yesterday's SAME window, not any other."""
    bars = []
    day = pd.Timestamp("2022-03-07")
    for offset in range(2):
        prices = {w: 100.0 for w in analysis.RELATIVE_WINDOWS}
        bars.extend(_minute_session(day + pd.Timedelta(days=offset), prices))
    rows = analysis.build_intraday_window_rows(_minute_frame(bars))

    day_two = rows[rows["Trading_Day"].eq(rows["Trading_Day"].max())]
    for _, row in day_two.iterrows():
        prior = rows[
            rows["Window_Name"].eq(row["Window_Name"])
            & rows["Trading_Day"].lt(row["Trading_Day"])
        ]
        assert row["Prior_Day_Same_Window_Range_Pct"] == pytest.approx(
            float(prior["Window_Range_Pct"].iloc[-1]))
        assert row["Window_Range_Ratio"] == pytest.approx(
            row["Window_Range_Pct"] / row["Prior_Day_Same_Window_Range_Pct"])


def test_next_window_return_is_the_following_window_in_the_same_session():
    """
    The signed follow-through target must be strictly after the row's own window and
    must not roll across the overnight gap into the next session.
    """
    rows = analysis.build_intraday_window_rows(_week_frame())
    ordered = rows.sort_values(["Trading_Day", "Window_Order"])

    last = ordered[ordered["Window_Name"].eq("1300_to_Close")]
    assert last["Next_Window_Return_Pct"].isna().all()
    for day, block in ordered.groupby("Trading_Day"):
        block = block.reset_index(drop=True)
        for i in range(len(block) - 1):
            assert block.loc[i, "Next_Window_Return_Pct"] == pytest.approx(
                block.loc[i + 1, "Window_Return_Pct"]), day


# ── The volatility-clustering baseline ─────────────────────────────────────────


def test_conditioners_containing_the_prior_session_are_flagged_on_the_ratio():
    """
    `Week_Range_So_Far_Pct` contains yesterday's range, which is the ratio's
    denominator, so its ratio spread is mechanical. It must be flagged on the ratio
    rows and left unflagged on the raw-range rows, which stay interpretable.
    """
    summary = pd.DataFrame([
        {"Split": "Train", "Window_Name": "0930_to_1300", "Conditioner": conditioner,
         "Conditioner_Value": value, "n": 100, "n_weeks": 20,
         "Window_Range_Pct_mean": mean, "Window_Range_Pct_ci_low": mean - 1,
         "Window_Range_Pct_ci_high": mean + 1,
         "Window_Range_Ratio_mean": mean, "Window_Range_Ratio_ci_low": mean - 1,
         "Window_Range_Ratio_ci_high": mean + 1,
         "Window_High_Excursion_Pct_mean": mean, "Window_High_Excursion_Pct_ci_low": mean - 1,
         "Window_High_Excursion_Pct_ci_high": mean + 1,
         "Window_Low_Excursion_Pct_mean": mean, "Window_Low_Excursion_Pct_ci_low": mean - 1,
         "Window_Low_Excursion_Pct_ci_high": mean + 1,
         "Window_Return_Pct_mean": mean, "Window_Return_Pct_ci_low": mean - 1,
         "Window_Return_Pct_ci_high": mean + 1,
         "Next_Window_Return_Pct_mean": mean, "Next_Window_Return_Pct_ci_low": mean - 1,
         "Next_Window_Return_Pct_ci_high": mean + 1}
        for conditioner in ["Week_Range_So_Far_Pct", "Day_Range_To_Window_Open_Pct"]
        for value, mean in [("P25 Low", 1.0), ("P75 High", 2.0)]
    ])

    spreads = analysis.window_conditioner_spreads(summary)

    overlapped = spreads[spreads["Conditioner"].eq("Week_Range_So_Far_Pct")]
    assert overlapped[overlapped["Target"].eq("Window_Range_Ratio")]["ratio_denominator_overlap"].all()
    assert not overlapped[overlapped["Target"].eq("Window_Range_Pct")]["ratio_denominator_overlap"].any()
    # A conditioner measured entirely inside the current session cannot overlap.
    clean = spreads[spreads["Conditioner"].eq("Day_Range_To_Window_Open_Pct")]
    assert not clean["ratio_denominator_overlap"].any()


def test_bootstrap_mean_ci_clusters_on_weeks():
    """
    The cluster bootstrap must widen the CI. Twenty correlated rows per week are not
    twenty independent draws, and the row bootstrap would understate the band.
    """
    rng = np.random.default_rng(0)
    weeks = np.repeat(np.arange(40), 20)
    # Each week has its own level, so rows within a week are strongly correlated.
    values = np.repeat(rng.normal(0, 1, 40), 20) + rng.normal(0, 0.05, 800)

    naive = analysis.bootstrap_mean_ci(values)
    clustered = analysis.bootstrap_mean_ci(values, clusters=weeks)

    assert clustered["n_clusters"] == 40
    assert naive["n_clusters"] == 800
    naive_width = naive["ci_high"] - naive["ci_low"]
    clustered_width = clustered["ci_high"] - clustered["ci_low"]
    assert clustered_width > naive_width * 2


# ── Runner wiring ──────────────────────────────────────────────────────────────


def test_week_context_is_exposed_as_a_module():
    assert "week_context" in analysis.MODULES
