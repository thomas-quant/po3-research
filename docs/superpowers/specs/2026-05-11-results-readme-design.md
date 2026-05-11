# Results-Oriented README Design

## Goal
Make the README results-first: show ES and NQ findings, chart-backed takeaways, and a reproducible TWAP/VWAP predictive matrix across all tracked intraday key levels.

## Findings Scope
- Include ES and NQ side-by-side where local parquet exists.
- Use conditional tendencies, not trading-rule language.
- Report train and OOS separately where predictive claims are made.
- Define predictive power as absolute delta from same-split baseline outcome rate.

## TWAP/VWAP Matrix
For each level/window pair:
- Globex Open with Globex_to_Midnight
- NY Midnight Open with Midnight_to_0930
- NY 09:30 Open with 0930_to_1300
- NY 13:00 Open with 1300_to_Close

For each TWAP/VWAP above-vs-below signal, measure delta vs baseline for:
- next-session bullish percentage
- daily close above the tracked level
- weekly bullish percentage
- weekly high Friday percentage
- weekly low Monday percentage

## Generated Artifacts
`output/examples/` should contain:
- existing ES gallery charts
- matching NQ gallery charts
- `es_twap_vwap_predictive_matrix.png`
- `nq_twap_vwap_predictive_matrix.png`
- `readme_findings_summary.csv`
- `readme_twap_vwap_predictive_summary.csv`

## README Structure
Top-level sections:
1. Key findings from current ES/NQ samples
2. TWAP/VWAP predictive matrix
3. ES/NQ result gallery
4. Reproduce the results

Collapsed sections retain:
- data/time conventions
- research module details
- output layout
- common commands
- project files

## Testing
- Unit tests verify ES/NQ gallery spec filenames, matrix spec filenames, summary column contracts, and script path execution.
- Run gallery script against local ES and NQ parquet to refresh PNG/CSV artifacts.
- Run project tests and py_compile before completion.
