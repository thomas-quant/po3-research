import numpy as np
import pandas as pd
import pytest

import analysis


def make_hourly_week(start, highs, lows, closes=None):
    idx = pd.date_range(start, periods=len(highs), freq="1h", tz="America/New_York")
    if closes is None:
        closes = [(h + l) / 2 for h, l in zip(highs, lows)]
    return pd.DataFrame(
        {
            "Open": closes,
            "High": highs,
            "Low": lows,
            "Close": closes,
            "Volume": [100] * len(highs),
        },
        index=idx,
    )


def test_build_event_rows_marks_single_events_and_already_occurred_flags_without_lookahead_features():
    df = make_hourly_week(
        "2024-01-01 00:00",
        highs=[100, 110, 105, 103],
        lows=[95, 96, 94, 90],
        closes=[98, 109, 100, 91],
    )

    rows = analysis.build_event_rows(df)

    assert rows["Is_Final_High_Event"].tolist() == [False, True, False, False]
    assert rows["Is_Final_Low_Event"].tolist() == [False, False, False, True]
    assert rows["High_Already_Formed"].tolist() == [False, True, True, True]
    assert rows["Low_Already_Formed"].tolist() == [False, False, False, True]
    assert rows["Week_High_So_Far"].tolist() == [100, 110, 110, 110]
    assert rows["Week_Low_So_Far"].tolist() == [95, 95, 94, 90]
    assert rows["Close_Location_Bucket"].tolist() == ["Middle", "Upper", "Middle", "Lower"]


def test_event_timing_distribution_matches_weekly_extreme_event_slots():
    week1 = make_hourly_week("2024-01-01 00:00", [100, 105, 103], [90, 91, 92])
    week2 = make_hourly_week("2024-01-08 00:00", [200, 198, 197], [190, 185, 188])
    rows = analysis.build_event_rows(pd.concat([week1, week2]))

    timing = analysis.event_timing_distribution(rows, "high", ["Weekday", "Session"])

    assert int(rows.groupby("Week_Start")["Is_Final_High_Event"].sum().max()) == 1
    assert int(rows.groupby("Week_Start")["Is_Final_Low_Event"].sum().max()) == 1
    assert timing["n"].sum() == 2
    assert timing.loc[timing["Weekday"].eq("Monday"), "pct"].sum() == 100.0


def test_bootstrap_probability_ci_is_deterministic_and_contains_observed_probability():
    values = pd.Series([True, True, False, False])

    ci1 = analysis.bootstrap_probability_ci(values, n_resamples=200, seed=7)
    ci2 = analysis.bootstrap_probability_ci(values, n_resamples=200, seed=7)

    assert ci1 == ci2
    assert ci1["probability"] == 50.0
    assert ci1["ci_low"] <= 50.0 <= ci1["ci_high"]


def test_train_oos_conditional_distribution_includes_bootstrap_ci_and_suppression_flag():
    rows = pd.DataFrame(
        {
            "Split": ["Train"] * 25 + ["OOS"] * 5,
            "Close_Location_Bucket": ["Upper"] * 30,
            "Is_Final_High_Event": [True] * 10 + [False] * 15 + [True] * 2 + [False] * 3,
        }
    )

    dist = analysis.conditional_event_distribution(
        rows,
        event_col="Is_Final_High_Event",
        condition_cols=["Close_Location_Bucket"],
        n_resamples=100,
        seed=3,
    )

    train = dist[dist["Split"].eq("Train")].iloc[0]
    oos = dist[dist["Split"].eq("OOS")].iloc[0]
    assert train["n"] == 25
    assert train["probability"] == 40.0
    assert train["is_sparse"] is False or train["is_sparse"] == False
    assert oos["n"] == 5
    assert oos["is_sparse"] is True or oos["is_sparse"] == True


def test_remaining_event_distribution_uses_only_rows_before_event_and_targets_final_slot():
    df = make_hourly_week(
        "2024-01-01 00:00",
        highs=[100, 102, 110, 105],
        lows=[95, 94, 93, 92],
        closes=[98, 99, 109, 100],
    )
    rows = analysis.build_event_rows(df)

    remaining = analysis.remaining_event_distribution(
        rows,
        event="high",
        checkpoint_cols=["Weekday"],
        target_cols=["Final_High_Hour"],
    )

    assert remaining["n"].sum() == 2
    assert remaining.iloc[0]["Final_High_Hour"] == 2
    assert remaining.iloc[0]["pct"] == 100.0


def test_build_weekly_open_revisit_rows_counts_monday_only_after_930_and_first_revisit():
    idx = pd.to_datetime([
        "2023-12-31 18:00", "2024-01-01 09:00", "2024-01-01 09:29",
        "2024-01-01 09:30", "2024-01-02 01:00", "2024-01-02 10:00",
    ]).tz_localize("America/New_York")
    df = pd.DataFrame(
        {
            "Open": [100, 101, 102, 103, 104, 105],
            "High": [100, 100.5, 100.5, 102.5, 104, 105],
            "Low": [100, 99.5, 99.5, 99.75, 99.5, 103],
            "Close": [100, 100, 100, 101, 100, 104],
            "Volume": [1] * 6,
        },
        index=idx,
    )
    weekly = analysis.build_weekly(df)

    rows = analysis.build_weekly_open_revisit_rows(df, weekly)

    assert rows.iloc[0]["Weekly_Open"] == 100
    assert rows.iloc[0]["Monday_Cross_Bars"] == 1
    assert rows.iloc[0]["Tuesday_Cross_Bars"] == 1
    assert rows.iloc[0]["First_Revisit_Timestamp"] == idx[3]
    assert rows.iloc[0]["First_Revisit_Weekday"] == "Monday"
    assert rows.iloc[0]["First_Revisit_Hour"] == 9


def test_apply_revisit_percentile_buckets_uses_train_quantiles_for_oos():
    rows = pd.DataFrame(
        {
            "Split": ["Train", "Train", "Train", "Train", "OOS"],
            "First_Revisit_Slot_Index": [10, 20, 30, 40, 35],
        }
    )

    out = analysis.apply_revisit_percentile_buckets(rows)

    assert out.loc[0, "First_Revisit_Timing_Bucket"] == "Early 0-25%"
    assert out.loc[1, "First_Revisit_Timing_Bucket"] == "Normal 25-75%"
    assert out.loc[2, "First_Revisit_Timing_Bucket"] == "Normal 25-75%"
    assert out.loc[3, "First_Revisit_Timing_Bucket"] == "Extreme Late 90-100%"
    assert out.loc[4, "First_Revisit_Is_Late_P80"] is True or out.loc[4, "First_Revisit_Is_Late_P80"] == True


def make_utc_1m_frame(times_et, opens, highs, lows, closes=None):
    idx_et = pd.to_datetime(times_et).tz_localize("America/New_York")
    if closes is None:
        closes = opens
    return pd.DataFrame(
        {
            "datetime_utc": idx_et.tz_convert("UTC"),
            "Open": opens,
            "High": highs,
            "Low": lows,
            "Close": closes,
            "Volume": [1] * len(opens),
        }
    )


def test_load_1m_source_supports_datetime_utc_schema(tmp_path):
    raw = make_utc_1m_frame(
        ["2024-01-02 09:30", "2024-01-02 09:31"],
        [100, 101], [101, 102], [99, 100], [100.5, 101.5],
    )
    path = tmp_path / "utc.parquet"
    raw.to_parquet(path)

    loaded = analysis.load_1m_source(path)

    assert str(loaded.index.tz) == "America/New_York"
    assert loaded.index[0].hour == 9
    assert loaded.index[0].minute == 30
    assert list(loaded["Open"]) == [100, 101]


def test_build_intraday_key_level_rows_maps_globex_to_next_day_and_excludes_defining_bar():
    raw = make_utc_1m_frame(
        [
            "2024-01-01 18:00",  # Globex open for Jan 2 trading day, defining bar only
            "2024-01-01 18:01",  # revisits Globex open
            "2024-01-02 00:00",  # Midnight defining bar only
            "2024-01-02 00:01",  # revisits midnight open
            "2024-01-02 09:30",  # NY open defining bar only
            "2024-01-02 09:31",  # revisits NY open
            "2024-01-02 13:00",  # 13:00 defining bar only
            "2024-01-02 13:01",  # revisits 13:00 open
            "2024-01-02 15:59",
        ],
        opens=[100, 101, 110, 111, 120, 121, 130, 131, 140],
        highs=[100, 101, 110, 111, 120, 121, 130, 131, 142],
        lows=[100, 99, 110, 109, 120, 119, 130, 129, 139],
        closes=[100, 100, 110, 110, 120, 120, 130, 130, 141],
    )
    df = raw.copy()
    df.index = pd.to_datetime(df["datetime_utc"], utc=True).dt.tz_convert("America/New_York")
    df = df[["Open", "High", "Low", "Close", "Volume"]]

    rows = analysis.build_intraday_key_level_rows(df)

    assert set(rows["Level_Name"]) == {"Globex_Open", "NY_Midnight_Open", "NY_0930_Open", "NY_1300_Open"}
    globex = rows[rows["Level_Name"].eq("Globex_Open")].iloc[0]
    assert str(globex["Trading_Day"].date()) == "2024-01-02"
    assert globex["Level_Value"] == 100
    assert globex["Touch_Bars_Total"] == 1
    assert globex["First_Revisit_Timestamp"].hour == 18
    assert globex["First_Revisit_Timestamp"].minute == 1
    ny0930 = rows[rows["Level_Name"].eq("NY_0930_Open")].iloc[0]
    assert ny0930["Touch_Bars_Total"] == 1
    assert ny0930["High_Already_Formed_At_Revisit"] is False or ny0930["High_Already_Formed_At_Revisit"] == False
    assert ny0930["Day_Close_Above_Level"] is True or ny0930["Day_Close_Above_Level"] == True


def test_intraday_key_level_distribution_and_outcomes_group_by_level_and_session():
    rows = pd.DataFrame(
        {
            "Split": ["Train", "Train", "Train"],
            "Level_Name": ["Globex_Open", "Globex_Open", "NY_0930_Open"],
            "Revisited": [True, False, True],
            "First_Revisit_Session": ["Asia", np.nan, "NY AM"],
            "Touch_Bars_Total": [3, 0, 1],
            "Day_Bullish": [True, False, True],
            "Day_Close_Above_Level": [True, False, True],
            "High_Already_Formed_At_Revisit": [False, False, True],
            "Low_Already_Formed_At_Revisit": [True, False, False],
        }
    )

    dist = analysis.intraday_level_revisit_distribution(rows)
    outcomes = analysis.intraday_level_path_outcomes(rows)

    globex = dist[dist["Level_Name"].eq("Globex_Open")].iloc[0]
    assert globex["revisit_pct"] == 50.0
    assert globex["avg_touch_bars"] == 1.5
    asia = outcomes[(outcomes["Level_Name"].eq("Globex_Open")) & (outcomes["First_Revisit_Session"].eq("Asia"))].iloc[0]
    assert asia["day_bullish_pct"] == 100.0


def test_load_and_resample_supports_datetime_utc_schema(tmp_path):
    raw = make_utc_1m_frame(
        ["2024-01-02 09:30", "2024-01-02 09:31"],
        [100, 101], [101, 102], [99, 100], [100.5, 101.5],
    )
    path = tmp_path / "utc_resample.parquet"
    raw.to_parquet(path)

    loaded = analysis.load_and_resample(path, "1h")

    assert str(loaded.index.tz) == "America/New_York"
    assert len(loaded) == 1
    assert loaded.iloc[0]["Open"] == 100
    assert loaded.iloc[0]["High"] == 102
    assert loaded.iloc[0]["Low"] == 99
    assert loaded.iloc[0]["Close"] == 101.5


def test_intraday_forward_touch_distribution_measures_touch_after_bucket_start():
    idx = pd.to_datetime([
        "2024-01-02 00:00",
        "2024-01-02 09:30",
        "2024-01-02 09:45",
        "2024-01-02 10:00",
        "2024-01-02 10:15",
    ]).tz_localize("America/New_York")
    df = pd.DataFrame(
        {
            "Open": [100, 110, 111, 112, 113],
            "High": [100, 111, 112, 113, 114],
            "Low": [100, 109, 99, 111, 112],
            "Close": [100, 110, 100, 112, 113],
            "Volume": [1] * 5,
        },
        index=idx,
    )

    out = analysis.intraday_level_forward_touch_distribution(df, bucket_freq="15min")
    midnight_930 = out[
        out["Level_Name"].eq("NY_Midnight_Open")
        & out["Bucket_Time"].eq("09:30")
    ].iloc[0]
    midnight_1000 = out[
        out["Level_Name"].eq("NY_Midnight_Open")
        & out["Bucket_Time"].eq("10:00")
    ].iloc[0]

    assert midnight_930["n"] == 1
    assert midnight_930["touch_pct"] == 100.0
    assert midnight_1000["touch_pct"] == 0.0


def test_intraday_forward_touch_distribution_supports_hour_buckets():
    idx = pd.to_datetime([
        "2024-01-02 00:00",
        "2024-01-02 09:30",
        "2024-01-02 10:01",
    ]).tz_localize("America/New_York")
    df = pd.DataFrame(
        {
            "Open": [100, 110, 111],
            "High": [100, 111, 112],
            "Low": [100, 109, 99],
            "Close": [100, 110, 100],
            "Volume": [1] * 3,
        },
        index=idx,
    )

    out = analysis.intraday_level_forward_touch_distribution(df, bucket_freq="1h")

    row = out[out["Level_Name"].eq("NY_Midnight_Open") & out["Bucket_Time"].eq("10:00")].iloc[0]
    assert row["touch_pct"] == 100.0


def test_apply_path_dependency_buckets_keeps_no_revisit_separate_and_uses_train_quantiles():
    rows = pd.DataFrame(
        {
            "Split": ["Train", "Train", "Train", "Train", "OOS", "OOS"],
            "Level_Name": ["NY_0930_Open"] * 6,
            "Revisited": [True, True, True, False, True, False],
            "Minutes_To_First_Revisit": [10, 20, 30, np.nan, 25, np.nan],
            "Touch_Bars_Total": [1, 5, 9, 0, 8, 0],
            "Post_Revisit_High_Excursion": [2, 5, 10, np.nan, 9, np.nan],
            "Post_Revisit_Low_Excursion": [1, 4, 8, np.nan, 7, np.nan],
        }
    )

    out = analysis.apply_path_dependency_buckets(rows)

    assert out.loc[3, "Minutes_To_First_Revisit_Bucket"] == "No Revisit"
    assert out.loc[0, "Minutes_To_First_Revisit_Bucket"] == "P25 Low"
    assert out.loc[2, "Minutes_To_First_Revisit_Bucket"] == "P75 High"
    assert out.loc[4, "Touch_Bars_Total_Bucket"] == "P75 High"
    assert out.loc[5, "Touch_Bars_Total_Bucket"] == "No Revisit"


def test_intraday_to_weekly_path_dependency_joins_weekly_outcomes_by_trading_week():
    intraday = pd.DataFrame(
        {
            "Trading_Day": pd.to_datetime(["2024-01-02", "2024-01-03"]).tz_localize("America/New_York"),
            "Split": ["Train", "Train"],
            "Level_Name": ["NY_0930_Open", "NY_0930_Open"],
            "Revisited": [True, True],
            "Minutes_To_First_Revisit": [10, 30],
            "Touch_Bars_Total": [1, 9],
            "Post_Revisit_High_Excursion": [2, 10],
            "Post_Revisit_Low_Excursion": [1, 8],
            "Day_Bullish": [True, False],
            "Day_Close_Above_Level": [True, False],
        }
    )
    week_start = pd.Timestamp("2024-01-01", tz="America/New_York")
    weekly = pd.DataFrame(
        {
            "Bull_Bear": ["Bullish"],
            "High_Weekday": ["Friday"],
            "Low_Weekday": ["Monday"],
            "High_Session": ["NY PM"],
            "Low_Session": ["NY AM"],
        },
        index=[week_start],
    )

    detail, summary = analysis.intraday_to_weekly_path_dependency(intraday, weekly)

    assert detail["Week_Start"].iloc[0] == week_start
    assert detail["Week_Bullish"].all()
    row = summary[summary["Metric_Bucket"].eq("P25 Low")].iloc[0]
    assert row["week_bullish_pct"] == 100.0
    assert row["weekly_high_friday_pct"] == 100.0
    assert row["weekly_low_monday_pct"] == 100.0


def test_build_relative_level_path_rows_counts_time_distance_and_next_session():
    idx = pd.to_datetime([
        "2024-01-02 00:00", "2024-01-02 00:01", "2024-01-02 09:29",
        "2024-01-02 09:30", "2024-01-02 09:31", "2024-01-02 13:00",
    ]).tz_localize("America/New_York")
    df = pd.DataFrame(
        {
            "Open": [100, 101, 102, 103, 104, 105],
            "High": [100, 102, 103, 104, 105, 106],
            "Low": [100, 99, 101, 102, 103, 104],
            "Close": [100, 101, 102, 103, 104, 105],
            "Volume": [1] * 6,
        },
        index=idx,
    )

    rows = analysis.build_relative_level_path_rows(df)
    row = rows[
        rows["Level_Name"].eq("NY_Midnight_Open")
        & rows["Window_Name"].eq("Midnight_to_0930")
    ].iloc[0]

    assert row["Bars_Total"] == 3
    assert row["Pct_Bars_Above_Level"] == round(2 / 3 * 100, 4)
    assert row["Pct_Bars_Touching_Level"] == round(2 / 3 * 100, 4)
    assert row["Window_Close_Above_Level"] is True or row["Window_Close_Above_Level"] == True
    assert row["Next_Session_Bullish"] is True or row["Next_Session_Bullish"] == True
    assert row["Day_Bullish"] is True or row["Day_Bullish"] == True


def test_apply_relative_path_buckets_uses_train_quantiles_per_level_window_feature():
    rows = pd.DataFrame(
        {
            "Split": ["Train", "Train", "Train", "OOS"],
            "Level_Name": ["NY_Midnight_Open"] * 4,
            "Window_Name": ["Midnight_to_0930"] * 4,
            "Pct_Bars_Below_Level": [10, 50, 90, 80],
            "Mean_Distance_Below": [1, 5, 9, 8],
        }
    )

    out = analysis.apply_relative_path_buckets(rows, features=["Pct_Bars_Below_Level", "Mean_Distance_Below"])

    assert out.loc[0, "Pct_Bars_Below_Level_Bucket"] == "P25 Low"
    assert out.loc[2, "Pct_Bars_Below_Level_Bucket"] == "P75 High"
    assert out.loc[3, "Pct_Bars_Below_Level_Bucket"] == "P75 High"
    assert out.loc[0, "Mean_Distance_Below_Bucket"] == "P25 Low"


def test_relative_path_to_weekly_outcomes_filters_monday_tuesday_and_joins_weekly():
    rows = pd.DataFrame(
        {
            "Trading_Day": pd.to_datetime(["2024-01-01", "2024-01-02", "2024-01-03"]).tz_localize("America/New_York"),
            "Split": ["Train"] * 3,
            "Weekday": ["Monday", "Tuesday", "Wednesday"],
            "Level_Name": ["NY_Midnight_Open"] * 3,
            "Window_Name": ["Midnight_to_0930"] * 3,
            "Pct_Bars_Below_Level": [10, 50, 90],
            "Day_Bullish": [True, False, True],
            "Day_Close_Above_Level": [True, False, True],
            "Next_Session_Bullish": [True, False, True],
            "Next_Session_Return_Pct": [1.0, -1.0, 2.0],
            "Day_Return_Pct": [2.0, -2.0, 3.0],
        }
    )
    week_start = pd.Timestamp("2024-01-01", tz="America/New_York")
    weekly = pd.DataFrame(
        {"Bull_Bear": ["Bullish"], "High_Weekday": ["Friday"], "Low_Weekday": ["Monday"], "High_Session": ["NY PM"], "Low_Session": ["NY AM"]},
        index=[week_start],
    )

    detail, summary = analysis.relative_path_to_weekly_outcomes(rows, weekly, features=["Pct_Bars_Below_Level"])

    assert set(detail["Weekday"]) == {"Monday", "Tuesday"}
    assert summary["n"].sum() == 2
    assert summary.iloc[0]["week_bullish_pct"] == 100.0


def test_composite_level_context_classifies_window_close_against_prior_levels():
    ctx = analysis.composite_level_context(105, {"Globex_Open": 100, "NY_Midnight_Open": 110})
    assert ctx["Composite_Level_State"] == "Between"
    assert ctx["Composite_Levels_Defined"] == 2
    assert ctx["Composite_N_Levels_Above_Close"] == 1
    assert ctx["Composite_N_Levels_Below_Close"] == 1
    assert ctx["Composite_Min_Distance_To_Level"] == 5
    assert ctx["Composite_Max_Distance_To_Level"] == 5

    assert analysis.composite_level_context(95, {"Globex_Open": 100})["Composite_Level_State"] == "Below_All"
    assert analysis.composite_level_context(115, {"Globex_Open": 100, "NY_Midnight_Open": 110})["Composite_Level_State"] == "Above_All"


def test_relative_path_rows_include_twap_vwap_and_composite_prior_levels_only():
    idx = pd.to_datetime([
        "2024-01-02 00:00", "2024-01-02 00:01", "2024-01-02 09:29",
        "2024-01-02 09:30", "2024-01-02 09:31", "2024-01-02 13:00",
    ]).tz_localize("America/New_York")
    df = pd.DataFrame(
        {
            "Open": [100, 101, 102, 103, 104, 105],
            "High": [100, 102, 103, 104, 105, 106],
            "Low": [100, 99, 101, 102, 103, 104],
            "Close": [100, 101, 102, 103, 104, 105],
            "Volume": [1, 2, 3, 4, 5, 6],
        },
        index=idx,
    )

    rows = analysis.build_relative_level_path_rows(df)
    row = rows[
        rows["Level_Name"].eq("NY_Midnight_Open")
        & rows["Window_Name"].eq("Midnight_to_0930")
    ].iloc[0]

    expected_twap = round((100 + 101 + 102) / 3, 4)
    typical = ((df.loc[idx[:3], "High"] + df.loc[idx[:3], "Low"] + df.loc[idx[:3], "Close"]) / 3)
    expected_vwap = round(float((typical * pd.Series([1, 2, 3], index=idx[:3])).sum() / 6), 4)
    assert row["Window_TWAP"] == expected_twap
    assert row["Window_VWAP"] == expected_vwap
    assert row["TWAP_Distance_To_Level"] == round(expected_twap - 100, 4)
    assert row["VWAP_Above_Level"] is True or row["VWAP_Above_Level"] == True
    assert row["Composite_Levels_Defined"] == 2  # Globex missing in fixture, Midnight + 09:30 available by window end
    assert row["Composite_Level_State"] == "Between"


def test_relative_path_summaries_include_composite_state_grouping():
    rows = pd.DataFrame(
        {
            "Trading_Day": pd.to_datetime(["2024-01-01", "2024-01-02"]).tz_localize("America/New_York"),
            "Split": ["Train", "Train"],
            "Weekday": ["Monday", "Tuesday"],
            "Level_Name": ["NY_Midnight_Open", "NY_Midnight_Open"],
            "Window_Name": ["Midnight_to_0930", "Midnight_to_0930"],
            "Pct_Bars_Below_Level": [10, 90],
            "TWAP_Distance_To_Level": [1, -1],
            "VWAP_Distance_To_Level": [1, -1],
            "Composite_Min_Distance_To_Level": [1, 2],
            "Composite_Max_Distance_To_Level": [3, 4],
            "Composite_N_Levels_Above_Close": [0, 2],
            "Composite_N_Levels_Below_Close": [2, 0],
            "Composite_Level_State": ["Above_All", "Below_All"],
            "Day_Bullish": [True, False],
            "Day_Close_Above_Level": [True, False],
            "Next_Session_Bullish": [True, False],
            "Next_Session_Return_Pct": [1.0, -1.0],
            "Day_Return_Pct": [2.0, -2.0],
        }
    )
    weekly = pd.DataFrame(
        {"Bull_Bear": ["Bullish"], "High_Weekday": ["Friday"], "Low_Weekday": ["Monday"], "High_Session": ["NY PM"], "Low_Session": ["NY AM"]},
        index=[pd.Timestamp("2024-01-01", tz="America/New_York")],
    )

    intraday = analysis.relative_path_intraday_outcomes(rows, features=["TWAP_Distance_To_Level"])
    _, weekly_summary = analysis.relative_path_to_weekly_outcomes(rows, weekly, features=["TWAP_Distance_To_Level"])

    assert "Composite_Level_State" in set(intraday["Feature"])
    assert "Above_All" in set(intraday["Feature_Bucket"])
    assert "Composite_Level_State" in set(weekly_summary["Feature"])
    assert "Below_All" in set(weekly_summary["Feature_Bucket"])


# ── Session-date weekday attribution ────────────────────────────────────────

def test_trading_weekday_uses_the_18_00_session_roll_not_the_calendar_day():
    sunday_evening = pd.Timestamp("2023-12-31 18:00", tz="America/New_York")
    monday_rth = pd.Timestamp("2024-01-01 09:30", tz="America/New_York")
    monday_evening = pd.Timestamp("2024-01-01 19:00", tz="America/New_York")

    assert analysis.trading_weekday(sunday_evening) == "Monday"
    assert analysis.trading_weekday(monday_rth) == "Monday"
    # The old calendar attribution called this "Monday", giving Monday two evening
    # sessions (~28.5h) against Friday's ~17h.
    assert analysis.trading_weekday(monday_evening) == "Tuesday"


def test_trading_weekday_gives_every_weekday_the_same_bar_count():
    idx = pd.date_range("2023-12-31 18:00", "2024-01-05 17:00", freq="1h", tz="America/New_York")
    counts = pd.Series(analysis.trading_weekday_index(idx)).value_counts()

    assert set(counts.index) == set(analysis.DAYS)
    assert counts.nunique() == 1


def test_trading_week_monday_is_unchanged_by_the_session_roll():
    week_start = pd.Timestamp("2024-01-01", tz="America/New_York")
    for ts in ["2023-12-31 18:00", "2024-01-01 09:30", "2024-01-01 19:00", "2024-01-05 16:00"]:
        assert analysis.trading_week_monday(pd.Timestamp(ts, tz="America/New_York")) == week_start


def test_build_weekly_attributes_an_evening_extreme_to_the_next_session_day():
    idx = pd.to_datetime([
        "2024-01-01 09:00", "2024-01-01 19:00", "2024-01-02 09:00",
    ]).tz_localize("America/New_York")
    df = pd.DataFrame(
        {"Open": [100, 100, 100], "High": [101, 105, 102], "Low": [99, 98, 90], "Close": [100, 100, 95], "Volume": [1, 1, 1]},
        index=idx,
    )

    weekly = analysis.build_weekly(df)

    assert weekly.iloc[0]["High_Weekday"] == "Tuesday"   # Monday 19:00 is Tuesday's session
    assert weekly.iloc[0]["Low_Weekday"] == "Tuesday"


# ── Next session ────────────────────────────────────────────────────────────

# One futures day with every key level present. Session labels per bar:
# 18:00 Other | 19:00-23:00 Asia | 00:00-08:00 London | 09:00-11:00 NY AM
# | 12:00-15:00 NY PM | 16:00-16:59 Other
SPARSE_DAY_CLOSES = {
    "2024-01-01 18:00": 100, "2024-01-01 19:00": 101, "2024-01-01 23:00": 102,
    "2024-01-02 00:00": 103, "2024-01-02 08:00": 104,
    "2024-01-02 09:00": 105, "2024-01-02 09:29": 106, "2024-01-02 09:30": 107, "2024-01-02 11:00": 108,
    "2024-01-02 12:00": 109, "2024-01-02 13:00": 110, "2024-01-02 15:00": 111,
    "2024-01-02 16:00": 112, "2024-01-02 16:59": 113,
}


def _sparse_day():
    idx = pd.to_datetime(list(SPARSE_DAY_CLOSES)).tz_localize("America/New_York")
    closes = list(SPARSE_DAY_CLOSES.values())
    return pd.DataFrame(
        {"Open": closes, "High": [c + 1 for c in closes], "Low": [c - 1 for c in closes], "Close": closes, "Volume": [1] * len(closes)},
        index=idx,
    )


def test_next_session_return_stops_at_the_next_session_not_the_day_close():
    rows = analysis.build_relative_level_path_rows(_sparse_day())
    row = rows[rows["Level_Name"].eq("NY_Midnight_Open") & rows["Window_Name"].eq("Midnight_to_0930")].iloc[0]

    # Window ends 09:30 inside NY AM; the next session block is NY PM (12:00, 15:00).
    # The day close (112 at 16:59) is in "Other" and must not be used.
    assert row["Next_Session_Return_Pct"] == pytest.approx((111 / 106 - 1) * 100)
    assert row["Next_Session_Bullish"] is True


def test_next_session_return_for_an_overnight_window_ends_at_the_london_close():
    rows = analysis.build_relative_level_path_rows(_sparse_day())
    row = rows[rows["Level_Name"].eq("Globex_Open") & rows["Window_Name"].eq("Globex_to_Midnight")].iloc[0]

    # Window ends at midnight inside Asia; next block is London (00:00, 08:00).
    assert row["Next_Session_Return_Pct"] == pytest.approx((104 / 102 - 1) * 100)


def test_next_session_return_is_nan_when_no_session_follows():
    rows = analysis.build_relative_level_path_rows(_sparse_day())
    row = rows[rows["Level_Name"].eq("NY_1300_Open") & rows["Window_Name"].eq("1300_to_Close")].iloc[0]

    assert pd.isna(row["Next_Session_Return_Pct"])
    assert pd.isna(row["Next_Session_Bullish"])


def test_next_session_bounds_skips_the_whole_current_block():
    sessions = np.array(["Asia", "Asia", "London", "London", "NY AM"], dtype=object)

    assert analysis._next_session_bounds(sessions, 0) == (2, 3)
    assert analysis._next_session_bounds(sessions, 2) == (4, 4)
    assert analysis._next_session_bounds(sessions, 4) is None


# ── Look-ahead level/window pairs ───────────────────────────────────────────

def test_relative_path_rows_flag_levels_that_postdate_the_window():
    rows = analysis.build_relative_level_path_rows(_sparse_day())
    flags = rows.set_index(["Level_Name", "Window_Name"])["Level_Defined_By_Window_End"]

    assert flags[("Globex_Open", "Globex_to_Midnight")] is True or flags[("Globex_Open", "Globex_to_Midnight")]
    assert not flags[("NY_1300_Open", "Globex_to_Midnight")]
    assert not flags[("NY_1300_Open", "Midnight_to_0930")]
    assert not flags[("NY_0930_Open", "Globex_to_Midnight")]


def test_drop_lookahead_level_windows_removes_only_undefined_pairs():
    rows = analysis.build_relative_level_path_rows(_sparse_day())

    kept = analysis.drop_lookahead_level_windows(rows)

    assert len(kept) == len(rows) - 3
    assert kept["Level_Defined_By_Window_End"].all()
    assert len(analysis.drop_lookahead_level_windows(rows, include_lookahead=True)) == len(rows)


def test_relative_path_summaries_exclude_lookahead_pairs_by_default():
    rows = analysis.build_relative_level_path_rows(_sparse_day())

    summary = analysis.relative_path_intraday_outcomes(rows, features=["Pct_Bars_Above_Level"])

    combos = set(zip(summary["Level_Name"], summary["Window_Name"]))
    assert ("NY_1300_Open", "Globex_to_Midnight") not in combos
    assert ("NY_Midnight_Open", "Midnight_to_0930") in combos


# ── VWAP without volume ─────────────────────────────────────────────────────

def test_zero_volume_window_yields_nan_vwap_rather_than_a_twap_copy():
    day = _sparse_day()
    day["Volume"] = 0

    rows = analysis.build_relative_level_path_rows(day)
    row = rows[rows["Level_Name"].eq("NY_Midnight_Open") & rows["Window_Name"].eq("Midnight_to_0930")].iloc[0]

    assert pd.isna(row["Window_VWAP"])
    assert pd.isna(row["VWAP_Distance_To_Level"])
    assert row["VWAP_Above_Level"] is None
    assert not pd.isna(row["Window_TWAP"])


# ── Train-only bucketing ────────────────────────────────────────────────────

def test_range_expansion_buckets_use_train_thresholds_only():
    rows = pd.DataFrame(
        {
            "Slot_Index": [0] * 6,
            "Split": ["Train"] * 3 + ["OOS"] * 3,
            "Developing_Range": [1.0, 2.0, 3.0, 100.0, 200.0, 300.0],
        }
    )

    buckets = analysis._range_expansion_buckets(rows)

    assert list(buckets[:3]) == ["Low", "Medium", "High"]
    # OOS rows sit far above every train tercile, so they are all "High".
    # Pooling the sample would have re-centred the thresholds on OOS data.
    assert list(buckets[3:]) == ["High", "High", "High"]


# ── Forward-touch ordering ──────────────────────────────────────────────────

def test_forward_touch_buckets_sort_from_the_18_00_session_open():
    idx = pd.date_range("2024-01-01 18:00", "2024-01-02 16:00", freq="1h", tz="America/New_York")
    df = pd.DataFrame(
        {"Open": 100.0, "High": 101.0, "Low": 99.0, "Close": 100.0, "Volume": 1.0},
        index=idx,
    )

    out = analysis.intraday_level_forward_touch_distribution(df, bucket_freq="1h")
    globex = out[out["Level_Name"].eq("Globex_Open")]

    assert list(globex["Bucket_Time"])[:3] == ["19:00", "20:00", "21:00"]
    assert list(globex["Bucket_Time"])[-1] == "16:00"
    assert globex["Bucket_Session_Minute"].is_monotonic_increasing


# ── Weekly targets keyed by weekday ─────────────────────────────────────────

def test_path_dependency_summary_keys_on_weekday_and_reports_week_counts():
    intraday = pd.DataFrame(
        {
            "Trading_Day": pd.to_datetime(["2024-01-01", "2024-01-05"]).tz_localize("America/New_York"),
            "Split": ["Train", "Train"],
            "Level_Name": ["NY_0930_Open", "NY_0930_Open"],
            "Revisited": [True, True],
            "Minutes_To_First_Revisit": [10, 30],
            "Touch_Bars_Total": [1, 9],
            "Post_Revisit_High_Excursion": [2, 10],
            "Post_Revisit_Low_Excursion": [1, 8],
            "Day_Bullish": [True, False],
            "Day_Close_Above_Level": [True, False],
        }
    )
    weekly = pd.DataFrame(
        {"Bull_Bear": ["Bullish"], "High_Weekday": ["Friday"], "Low_Weekday": ["Monday"], "High_Session": ["NY PM"], "Low_Session": ["NY AM"]},
        index=[pd.Timestamp("2024-01-01", tz="America/New_York")],
    )

    _, summary = analysis.intraday_to_weekly_path_dependency(intraday, weekly)

    assert "Weekday" in summary.columns
    assert set(summary["Weekday"]) == {"Monday", "Friday"}
    # Both days belong to the same week, so a weekly label is one observation.
    assert summary["n_weeks"].max() == 1


def test_weekly_open_revisit_outcomes_report_monday_extremes():
    rows = pd.DataFrame(
        {
            "Split": ["Train", "Train"],
            "Revisited_Weekly_Open": [True, True],
            "First_Revisit_Timing_Bucket": ["Early 0-25%", "Early 0-25%"],
            "Bull_Bear": ["Bullish", "Bearish"],
            "High_Weekday": ["Monday", "Friday"],
            "Low_Weekday": ["Monday", "Monday"],
            "Close_Above_Weekly_Open": [True, False],
        }
    )

    out = analysis.weekly_open_revisit_outcomes(rows)

    assert out.iloc[0]["low_monday_pct"] == 100.0
    assert out.iloc[0]["high_monday_pct"] == 50.0


# ── Cluster bootstrap ───────────────────────────────────────────────────────

def test_cluster_bootstrap_is_wider_than_the_row_bootstrap_on_correlated_rows():
    # 10 weeks, 20 bars each; every bar in a week carries that week's outcome.
    outcomes = [True] * 5 + [False] * 5
    values = pd.Series([o for o in outcomes for _ in range(20)])
    weeks = pd.Series([w for w in range(10) for _ in range(20)])

    naive = analysis.bootstrap_probability_ci(values, n_resamples=400, seed=1)
    clustered = analysis.bootstrap_probability_ci(values, n_resamples=400, seed=1, clusters=weeks)

    assert naive["probability"] == clustered["probability"] == 50.0
    assert naive["n"] == clustered["n"] == 200
    assert clustered["n_clusters"] == 10
    naive_width = naive["ci_high"] - naive["ci_low"]
    clustered_width = clustered["ci_high"] - clustered["ci_low"]
    assert clustered_width > naive_width * 2


def test_conditional_event_distribution_clusters_on_week_and_flags_sparse_by_week():
    rows = pd.DataFrame(
        {
            "Split": ["Train"] * 60,
            "Week_Start": [w for w in range(3) for _ in range(20)],
            "Close_Location_Bucket": ["Upper"] * 60,
            "Is_Final_High_Event": [True] * 20 + [False] * 40,
        }
    )

    dist = analysis.conditional_event_distribution(
        rows, event_col="Is_Final_High_Event", condition_cols=["Close_Location_Bucket"], n_resamples=100, seed=3
    )
    row = dist.iloc[0]

    assert row["n"] == 60
    assert row["n_clusters"] == 3
    # 3 independent weeks is sparse even though 60 bar-rows is not.
    assert bool(row["is_sparse"]) is True


# ── DST-safe session dates ──────────────────────────────────────────────────

@pytest.mark.parametrize("evening,expected_session_date", [
    ("2024-11-03 18:00", "2024-11-04"),   # clocks went back that morning
    ("2024-11-03 23:59", "2024-11-04"),
    ("2024-03-10 18:00", "2024-03-11"),   # clocks went forward that morning
    ("2024-01-07 18:00", "2024-01-08"),   # ordinary Sunday
])
def test_session_date_lands_on_local_midnight_across_dst(evening, expected_session_date):
    ts = pd.Timestamp(evening, tz="America/New_York")

    scalar = analysis.intraday_trading_day(ts)
    vector = analysis.intraday_trading_day_index(pd.DatetimeIndex([ts]))[0]

    for value in (scalar, vector):
        assert str(value.date()) == expected_session_date
        assert (value.hour, value.minute) == (0, 0)


def test_dst_evening_and_next_morning_share_one_trading_day():
    idx = pd.to_datetime(["2024-11-03 18:00", "2024-11-03 20:00", "2024-11-04 00:00", "2024-11-04 09:30"]).tz_localize("America/New_York")

    days = analysis.intraday_trading_day_index(idx)

    assert days.nunique() == 1
    assert analysis.trading_week_monday_index(idx).nunique() == 1
