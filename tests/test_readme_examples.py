from pathlib import Path

from scripts.build_readme_examples import (
    DEFAULT_DATA_PATH,
    DEFAULT_OUTPUT_DIR,
    FINDINGS_COLUMNS,
    TWAP_VWAP_COLUMNS,
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
        "Abs_Best_Delta_Ppt",
        "Best_Side",
        "n_above",
        "n_below",
    ]


def test_parse_symbol_data_args_supports_repeated_pairs():
    pairs = parse_symbol_data_args(["ES=data/es_1m.parquet", "NQ=data/nq_1m.parquet"])

    assert pairs == [("ES", Path("data/es_1m.parquet")), ("NQ", Path("data/nq_1m.parquet"))]
