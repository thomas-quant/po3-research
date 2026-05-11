# Results-Oriented README Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Rebuild README around chart-backed ES/NQ findings and TWAP/VWAP predictive matrices.

**Architecture:** Extend `scripts/build_readme_examples.py` so README artifacts are reproducible from local parquet. Add data-light tests for filenames and summary schemas. Rewrite README after generated summaries exist.

**Tech Stack:** Python, pandas, matplotlib, pytest, Markdown `<details>`.

---

## File Structure
- Modify `scripts/build_readme_examples.py`: multi-symbol support, findings CSVs, TWAP/VWAP matrix chart.
- Modify `tests/test_readme_examples.py`: expected file lists and summary schema tests.
- Modify `README.md`: findings-first presentation.
- Add/update `output/examples/*`: ES/NQ charts and summary CSVs.

### Task 1: Tests for results artifacts
- [ ] Add failing tests for ES/NQ specs, TWAP/VWAP matrix filenames, and summary schemas.
- [ ] Run targeted tests and confirm failure.
- [ ] Implement metadata/schema functions only.
- [ ] Run targeted tests and confirm pass.

### Task 2: Generate ES/NQ findings and matrices
- [ ] Extend CLI to accept repeated `--symbol-data SYMBOL=PATH` values while preserving old `--symbol --data` mode.
- [ ] Build per-symbol gallery images.
- [ ] Build `readme_findings_summary.csv`.
- [ ] Build `readme_twap_vwap_predictive_summary.csv`.
- [ ] Build one TWAP/VWAP matrix PNG per symbol.
- [ ] Run script for ES and NQ local parquet.

### Task 3: Rewrite README around results
- [ ] Place key ES/NQ numeric takeaways above implementation detail.
- [ ] Add TWAP/VWAP matrix interpretation with conditional-tendency wording.
- [ ] Add ES/NQ gallery links.
- [ ] Keep methods and commands collapsed.

### Task 4: Verify
- [ ] Run `python3 -m pytest -q tests/test_event_research.py tests/test_readme_examples.py`.
- [ ] Run `python3 -m py_compile analysis.py scripts/build_readme_examples.py`.
- [ ] Confirm git status has no data files and output tracking limited to `output/examples/` artifacts.
