from pathlib import Path

from scripts.build_readme_examples import DEFAULT_DATA_PATH, DEFAULT_OUTPUT_DIR, gallery_specs


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
