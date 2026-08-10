import subprocess
import sys
from pathlib import Path

import pytest

import analysis
from po3_research.research import MODULES, build_arg_parser, module_output_dir, run_research


def test_module_list_matches_the_documented_research_modules():
    assert MODULES == [
        "weekly_charts",
        "weekly_events",
        "weekly_open_revisit",
        "intraday_levels",
        "path_dependency",
        "relative_path",
    ]


def test_cli_defaults_cover_every_module():
    args = build_arg_parser().parse_args([])

    assert args.symbol == "ES"
    assert args.modules == MODULES
    assert args.resample_to == "1h"


def test_cli_accepts_a_symbol_and_module_subset():
    args = build_arg_parser().parse_args(
        ["--symbol", "NQ", "--data", "data/nq_1m.parquet", "--modules", "relative_path", "intraday_levels"]
    )

    assert args.symbol == "NQ"
    assert args.data == "data/nq_1m.parquet"
    assert args.modules == ["relative_path", "intraday_levels"]


def test_output_directories_are_symbol_scoped():
    es = module_output_dir("intraday_levels", "ES", Path("output/research_events"))
    nq = module_output_dir("intraday_levels", "NQ", Path("output/research_events"))

    assert es != nq
    assert es.name == "intraday_levels_es"
    assert nq.name == "intraday_levels_nq"


def test_run_research_rejects_unknown_modules(tmp_path):
    with pytest.raises(ValueError, match="Unknown module"):
        run_research(symbol="ES", data_path="data/es_1m.parquet", output_dir=tmp_path, modules=["nope"])


def test_module_is_executable(tmp_path):
    result = subprocess.run(
        [sys.executable, "-m", "po3_research", "--help"],
        check=False, capture_output=True, text=True,
    )

    assert result.returncode == 0
    assert "--modules" in result.stdout


def test_analysis_wrapper_keeps_its_own_module_identity():
    assert Path(analysis.__file__).name == "analysis.py"
    assert "Backward-compatible entrypoint" in analysis.__doc__
    assert analysis.build_weekly is not None
