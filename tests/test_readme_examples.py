from pathlib import Path

import numpy as np
import pandas as pd

from scripts.build_readme_examples import (
    DEFAULT_DATA_PATH,
    DEFAULT_OUTPUT_DIR,
    FINDINGS_COLUMNS,
    TWAP_VWAP_COLUMNS,
    _build_twap_vwap_summary,
    _is_same_level_close_state,
    _weekday_scope,
    gallery_specs,
    matrix_specs,
    parse_symbol_data_args,
)


def test_gallery_specs_are_stable_core_six():
    specs = gallery_specs("ES")

    assert [spec.filename for spec in specs] == [
        "es_weekly_low_day_distribution.png",
        "es_weekly_high_day_distribution.png",
        "es_weekly_extreme_hour_distribution.png",
        "es_weekly_day_session_heatmap.png",
        "es_midnight_open_forward_touch_15m.png",
        "es_relative_path_context_summary.png",
    ]
    assert len({spec.title for spec in specs}) == 6


def test_default_paths_target_local_es_gallery():
    assert DEFAULT_DATA_PATH == Path("data/es_1m.parquet")
    assert DEFAULT_OUTPUT_DIR == Path("output/examples")


def test_script_runs_help_when_executed_by_path():
    import subprocess
    import sys

    result = subprocess.run(
        [sys.executable, "scripts/build_readme_examples.py", "--help"],
        check=False,
        capture_output=True,
        text=True,
    )

    assert result.returncode == 0
    assert "Build README example PNGs" in result.stdout


def test_gallery_specs_are_symbol_specific_for_nq():
    specs = gallery_specs("NQ")

    assert [spec.filename for spec in specs] == [
        "nq_weekly_low_day_distribution.png",
        "nq_weekly_high_day_distribution.png",
        "nq_weekly_extreme_hour_distribution.png",
        "nq_weekly_day_session_heatmap.png",
        "nq_midnight_open_forward_touch_15m.png",
        "nq_relative_path_context_summary.png",
    ]


def test_matrix_specs_are_stable_per_symbol():
    assert [spec.filename for spec in matrix_specs("ES")] == ["es_twap_vwap_predictive_matrix.png"]
    assert [spec.filename for spec in matrix_specs("NQ")] == ["nq_twap_vwap_predictive_matrix.png"]


def test_summary_column_contracts_are_stable():
    assert FINDINGS_COLUMNS == [
        "Symbol",
        "Metric",
        "Split",
        "Segment",
        "Value",
        "Context",
    ]
    assert TWAP_VWAP_COLUMNS == [
        "Symbol",
        "Split",
        "Level_Name",
        "Window_Name",
        "Signal",
        "Target",
        "Baseline_Pct",
        "Above_Pct",
        "Below_Pct",
        "Above_Delta_Ppt",
        "Below_Delta_Ppt",
        "End_State_Baseline_Pct",
        "End_State_Adjusted_Delta_Ppt",
        "Abs_Best_Delta_Ppt",
        "Best_Side",
        "Weekday_Scope",
        "n_above",
        "n_below",
        "n_weeks",
    ]


def test_parse_symbol_data_args_supports_repeated_pairs():
    pairs = parse_symbol_data_args(["ES=data/es_1m.parquet", "NQ=data/nq_1m.parquet"])

    assert pairs == [("ES", Path("data/es_1m.parquet")), ("NQ", Path("data/nq_1m.parquet"))]


def test_summary_targets_are_not_same_level_close_state():
    forbidden = {"Day close above level"}
    expected_future_targets = {
        "Next session bullish",
        "Next session positive return",
        "Day bullish",
        "Week bullish",
        "Weekly high Friday",
        "Weekly low Monday",
    }

    from scripts.build_readme_examples import TWAP_VWAP_TARGETS

    targets = {label for label, _ in TWAP_VWAP_TARGETS}
    assert targets.isdisjoint(forbidden)
    assert expected_future_targets.issubset(targets)


def _twap_rows():
    """Two Globex windows per weekday across two weeks, one bullish / one bearish."""
    days = pd.to_datetime([
        "2024-01-01", "2024-01-02", "2024-01-03", "2024-01-04", "2024-01-05",
        "2024-01-08", "2024-01-09", "2024-01-10", "2024-01-11", "2024-01-12",
    ]).tz_localize("America/New_York")
    n = len(days)
    return pd.DataFrame(
        {
            "Trading_Day": days,
            "Split": ["Train"] * n,
            "Weekday": ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday"] * 2,
            "Level_Name": ["Globex_Open"] * n,
            "Window_Name": ["Globex_to_Midnight"] * n,
            "TWAP_Above_Level": [True, False] * (n // 2),
            "VWAP_Above_Level": [True, False] * (n // 2),
            "Window_Close_Above_Level": [True, False] * (n // 2),
            "Next_Session_Bullish": [True, True, False, False, True, False, True, True, False, False],
            "Next_Session_Return_Pct": [1.0, np.nan] * (n // 2),
            "Day_Bullish": [True, False] * (n // 2),
            "Day_Close_Above_Level": [True, False] * (n // 2),
        }
    )


def _weekly_frame():
    return pd.DataFrame(
        {"Bull_Bear": ["Bullish", "Bearish"], "High_Weekday": ["Friday", "Monday"], "Low_Weekday": ["Monday", "Friday"]},
        index=pd.to_datetime(["2024-01-01", "2024-01-08"]).tz_localize("America/New_York"),
    )


def test_weekday_scope_restricts_weekly_targets_to_early_week_rows():
    assert _weekday_scope("Week_Bullish") == "Mon-Tue"
    assert _weekday_scope("Weekly_High_Friday") == "Mon-Tue"
    assert _weekday_scope("Day_Bullish") == "All"


def test_same_level_close_state_detects_the_globex_day_bullish_tautology():
    rows = pd.DataFrame({"Day_Bullish": [True, False], "Day_Close_Above_Level": [True, False], "Week_Bullish": [True, True]})

    assert _is_same_level_close_state(rows, "Day_Bullish") is True
    assert _is_same_level_close_state(rows, "Week_Bullish") is False


def test_globex_day_bullish_target_is_dropped_as_tautological():
    summary = _build_twap_vwap_summary("ES", _twap_rows(), _weekly_frame())

    assert "Day bullish" not in set(summary["Target"])
    assert "Week bullish" in set(summary["Target"])


def test_weekly_targets_are_scored_on_monday_tuesday_rows_only():
    summary = _build_twap_vwap_summary("ES", _twap_rows(), _weekly_frame())

    weekly = summary[summary["Target"].eq("Weekly high Friday")].iloc[0]
    daily = summary[summary["Target"].eq("Next session bullish")].iloc[0]

    assert weekly["Weekday_Scope"] == "Mon-Tue"
    assert weekly["n_above"] + weekly["n_below"] == 4      # Mon+Tue over two weeks
    assert weekly["n_weeks"] == 2
    assert daily["Weekday_Scope"] == "All"
    assert daily["n_above"] + daily["n_below"] == 10


def test_missing_next_session_returns_are_excluded_not_counted_as_negative():
    summary = _build_twap_vwap_summary("ES", _twap_rows(), _weekly_frame())

    row = summary[summary["Target"].eq("Next session positive return")].iloc[0]

    # Half the rows have no following session. They must not be scored as 0%.
    assert row["Baseline_Pct"] == 100.0


def test_targets_with_no_observations_are_not_emitted():
    rows = _twap_rows()
    rows["Next_Session_Bullish"] = np.nan
    rows["Next_Session_Return_Pct"] = np.nan

    summary = _build_twap_vwap_summary("ES", rows, _weekly_frame())

    assert "Next session bullish" not in set(summary["Target"])
    assert "Next session positive return" not in set(summary["Target"])
    assert summary["Baseline_Pct"].notna().all()


def test_globex_midnight_findings_score_both_targets_per_split():
    days = pd.to_datetime(["2024-01-02", "2024-01-03", "2024-01-04", "2024-01-05"]).tz_localize("America/New_York")
    rows = pd.DataFrame(
        {
            "Trading_Day": list(days) * 2,
            "Split": ["Train"] * 8,
            "Window_Name": ["Globex_to_Midnight"] * 8,
            "Level_Name": ["Globex_Open"] * 4 + ["NY_Midnight_Open"] * 4,
            "Level_Value": [100, 100, 100, 100] + [101, 99, 101, 99],
            "Day_Bullish": [True, False, True, False] * 2,
            "Day_Close_Above_Level": [True, False, True, False] * 2,
        }
    )

    from scripts.build_readme_examples import build_globex_midnight_findings

    out = pd.DataFrame(build_globex_midnight_findings("ES", rows))

    assert set(out["Metric"]) == {
        "Globex to midnight state — Full day bullish",
        "Globex to midnight state — Midnight to close positive",
    }
    above = out[out["Metric"].str.endswith("Full day bullish") & out["Segment"].eq("midnight above globex")].iloc[0]
    below = out[out["Metric"].str.endswith("Full day bullish") & out["Segment"].eq("midnight at or below globex")].iloc[0]
    assert above["Value"] == 100.0
    assert below["Value"] == 0.0
    assert "n=2" in above["Context"]
