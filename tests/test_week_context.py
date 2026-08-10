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
        assert row["Window_Log_Range_Ratio"] == pytest.approx(
            np.log(row["Window_Range_Ratio"]))


def test_log_range_ratio_is_symmetric_and_survives_a_zero_range_window():
    """
    The log form is the headline because the level form is right-skewed: a mean over
    ratios is dragged by the tail, and the widest bucket is dragged hardest. Halving
    and doubling must be equal and opposite, and a zero-range window — a halt, not a
    real contraction — must drop out rather than becoming -inf.
    """
    rows = pd.DataFrame({
        "Window_Range_Pct": [1.0, 2.0, 0.0],
        "Prior_Day_Same_Window_Range_Pct": [2.0, 1.0, 1.0],
    })
    ratio = rows["Window_Range_Pct"] / rows["Prior_Day_Same_Window_Range_Pct"]
    log_ratio = np.log(ratio.replace(0.0, np.nan))

    assert log_ratio.iloc[0] == pytest.approx(-log_ratio.iloc[1])
    assert np.isnan(log_ratio.iloc[2])
    # The level form has no such symmetry: 0.5 and 2.0 average to 1.25, not 1.0.
    assert ratio.iloc[:2].mean() == pytest.approx(1.25)
    assert log_ratio.iloc[:2].mean() == pytest.approx(0.0)


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
         "Window_Log_Range_Ratio_mean": mean, "Window_Log_Range_Ratio_ci_low": mean - 1,
         "Window_Log_Range_Ratio_ci_high": mean + 1,
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
    assert overlapped[overlapped["Target"].eq("Window_Log_Range_Ratio")]["ratio_denominator_overlap"].all()
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


# ── Nested closing windows ─────────────────────────────────────────────────────


def _closing_session(session_date, prices) -> list:
    """Minute bars through the afternoon, so the nested closing windows are populated."""
    day = pd.Timestamp(session_date).normalize()
    bars = []
    for hour, minute in [(13, 0), (13, 30), (14, 0), (14, 30), (15, 0), (15, 20),
                         (15, 40), (15, 50), (15, 55), (16, 30)]:
        price = prices.get((hour, minute), 100.0)
        stamp = day + pd.Timedelta(hours=hour, minutes=minute)
        bars.append((stamp, price, price + 1.0, price - 1.0, price))
    # A Globex bar so the session has an open before 13:00.
    bars.insert(0, (day - pd.Timedelta(hours=6), 100.0, 101.0, 99.0, 100.0))
    return bars


def _closing_frame(n_sessions: int = 6) -> pd.DataFrame:
    bars = []
    day = pd.Timestamp("2022-03-07")
    for _ in range(n_sessions):
        while day.dayofweek >= 5:
            day += pd.Timedelta(days=1)
        bars.extend(_closing_session(day, {}))
        day += pd.Timedelta(days=1)
    return _minute_frame(bars)


def test_nested_windows_never_take_a_prior_window_from_the_tiling():
    """
    A nested window sits inside a tiling window, so a shift(1) over the combined set
    would hand it an overlapping neighbour. Those columns must stay NaN, and the
    tiling rows must be untouched by the nested rows being appended.
    """
    rows = analysis.build_intraday_window_rows(_closing_frame())

    nested = rows[rows["Window_Name"].isin(analysis.WEEK_CONTEXT_NESTED_WINDOWS)]
    assert not nested.empty
    assert nested["Prior_Window_Range_Pct"].isna().all()
    assert nested["Prior_Window_Return_Pct"].isna().all()
    assert nested["Next_Window_Return_Pct"].isna().all()
    # And the nested rows do carry their own explicit prior stretches.
    for column in analysis.NESTED_PRIOR_CONDITIONERS:
        assert nested[column].notna().any(), column
    # Tiling rows never carry the nested-only conditioners.
    tiling = rows[rows["Window_Name"].isin(analysis.RELATIVE_WINDOWS)]
    for column in analysis.NESTED_PRIOR_CONDITIONERS:
        assert tiling[column].isna().all(), column


def test_nested_prior_stretches_do_not_overlap_their_target_window():
    """
    The whole point of an explicit prior stretch is that it ends before the target
    opens. If either definition leaked into the window it conditions, the conditioner
    would contain its own target.
    """
    for name, spec in analysis.WEEK_CONTEXT_NESTED_WINDOWS.items():
        (w_start, _w_end) = spec["window"]
        for key in ("preceding", "equal_length"):
            _p_start, p_end = spec[key]
            assert p_end <= w_start, f"{name}.{key} ends at {p_end}, window opens {w_start}"


def test_closing_ten_minutes_is_narrower_than_the_hour_that_contains_it():
    """A sanity check that the two nested windows are the stretches they claim to be."""
    rows = analysis.build_intraday_window_rows(_closing_frame())

    hour = rows[rows["Window_Name"].eq("1500_to_1600")]
    ten = rows[rows["Window_Name"].eq("1550_to_1600")]
    assert not hour.empty and not ten.empty
    assert int(ten["Bars_Total"].max()) < int(hour["Bars_Total"].max())
