# README Gallery Cleanup Design

## Goal
Restructure `README.md` into a short, readable overview with collapsible detail sections, and add a reproducible script that builds six tracked README example PNGs.

## Scope
- Add `scripts/build_readme_examples.py`.
- Generate six ES-focused example PNGs under `output/examples/`.
- Rewrite `README.md` with a compact top-level flow and `<details>` sections for deeper material.
- Keep project framing research-first, not trading rules.
- Do not commit local data or broad generated `output/` artifacts beyond `output/examples/*.png`.

## Core Six Gallery
1. Weekly low weekday distribution.
2. Weekly high weekday distribution.
3. Weekly extreme hour distribution.
4. Weekly day × session heatmap.
5. Midnight-open forward-touch probability by 15-minute bucket.
6. Relative path / key-level context summary.

## Chart Design
The README-specific gallery should be more explanatory than standard research charts:
- Use clear titles, subtitles, sample counts, and axis labels.
- Include brief chart footnotes where definitions matter.
- Use consistent color palette and DPI.
- Save files with stable names so README links do not churn.
- Use `matplotlib` only; no new visualization dependency.

## Script Design
`scripts/build_readme_examples.py` should:
- Import existing helpers from `po3_research.research`.
- Accept CLI options for symbol, 1-minute parquet path, output directory, and resample interval.
- Default to ES paths: `data/es_1m.parquet`, `output/examples/`.
- Recompute needed research tables locally from parquet.
- Produce only the six gallery images.
- Use Matplotlib `Agg` backend for headless execution.

## README Structure
Top-level README should show:
- Purpose and research scope.
- Quick start.
- Example gallery.
- Data expectations.
- How to regenerate README examples.

Collapsible sections should hold:
- Data schema and timezone/session conventions.
- Detailed module descriptions.
- Output tree.
- Testing and verification.
- Research caveat.

## Testing
- Unit-test gallery spec/config behavior without requiring local parquet data.
- Run existing regression tests and `py_compile` before completion.
- If PNG behavior changes, run README gallery script against ES data and verify six PNGs exist.
