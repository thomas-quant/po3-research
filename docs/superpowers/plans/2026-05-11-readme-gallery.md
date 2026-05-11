# README Gallery Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a reproducible six-image README gallery and rewrite README into readable collapsible sections.

**Architecture:** Add a focused script under `scripts/` for docs-only chart generation. Keep tests data-light by verifying gallery metadata and CLI defaults. README links stable files in `output/examples/`.

**Tech Stack:** Python, pandas/matplotlib existing stack, pytest, Markdown `<details>`.

---

## File Structure
- Create `scripts/build_readme_examples.py`: CLI + chart builders for six README example PNGs.
- Create `tests/test_readme_examples.py`: unit tests for gallery spec/default behavior that do not need parquet data.
- Modify `README.md`: collapsible docs + gallery.
- Modify `.gitignore` if needed: ensure `output/examples/*.png` remains trackable.
- Add generated PNGs under `output/examples/`.

### Task 1: Data-light tests for gallery script
- [ ] Write tests that assert six stable gallery specs and ES defaults.
- [ ] Run targeted test and verify it fails because script does not exist.
- [ ] Implement minimal spec/default functions in `scripts/build_readme_examples.py`.
- [ ] Run targeted test and verify pass.

### Task 2: Implement gallery chart generation
- [ ] Add CLI parsing and Matplotlib `Agg` backend.
- [ ] Add chart functions for core six examples using existing research helpers.
- [ ] Generate six PNGs from ES local data into `output/examples/`.
- [ ] Verify exactly six expected files exist and are non-empty.

### Task 3: Rewrite README
- [ ] Replace README with concise overview + gallery + collapsible sections.
- [ ] Include regeneration command for `scripts/build_readme_examples.py`.
- [ ] Preserve research-first caveat and project conventions.

### Task 4: Verification
- [ ] Run `python3 -m pytest -q tests/test_event_research.py tests/test_readme_examples.py`.
- [ ] Run `python3 -m py_compile analysis.py scripts/build_readme_examples.py`.
- [ ] Inspect git diff for data/output leaks beyond `output/examples/*.png`.
