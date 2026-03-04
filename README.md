# PO3 Research — Weekly Market Pattern Analysis

Research analyzing when the high/low of a week will most likely form, using ES (S&P 500 E-mini) and NQ (Nasdaq-100 E-mini) 1-minute OHLCV data from 2020–2025.

The resample frequency is configurable (1h, 4h, 1D, etc.), and session-based patterns are computed from the raw timestamps.

---

## Project Overview

**Main Script:** `analysis.py` — Unified monolith script that consolidates all analysis functionality.

**Dataset:** ~1.85M rows of 1-minute candlesticks (ES and NQ) covering 5+ years of market data.

**Key Finding:** Bullish weeks show a strong pattern where LOWs form on Mondays (~59%) and HIGHs form on Fridays (~61%), suggesting a "buy-low-Monday, sell-high-Friday" opportunity.

---

## Data Structure

### Data Files
- `data/es_1m.parquet` — E-mini S&P 500, 1-minute OHLCV
- `data/nq_1m.parquet` — E-mini Nasdaq-100, 1-minute OHLCV

### Parquet Schema
| Column | Type | Notes |
|--------|------|-------|
| `DateTime_ET` | datetime64 | Eastern Time (no timezone), must be localized with `tz_localize("America/New_York", ambiguous="infer")` |
| `Open` | float64 | Opening price |
| `High` | float64 | High price |
| `Low` | float64 | Low price |
| `Close` | float64 | Closing price |
| `Volume` | int64 | Volume |
| `DateTime_UTC` | datetime64 | UTC equivalent |
| `session` | string | Trading session (Asia, London, NY AM, NY PM, Other) |
| `window` | string | Time window within session |

---

## Script Usage

### Basic Configuration

Open `analysis.py` and modify the CONFIG section:

```python
SYMBOL      = "ES"                    # label used in chart titles
DATA_PATH   = "data/es_1m.parquet"   # 1-minute OHLCV parquet
RESAMPLE_TO = "1h"                    # "1h", "4h", "1D", etc.
OUTPUT_DIR  = Path("output")          # where charts are saved
```

### Running the Script

```bash
python analysis.py
```

**Output:**
- Weekly summary table (printed to console)
- 7 chart files saved to `output/` directory (PNG format):
  - `1_weekly_high_day.png` — HIGH distribution by weekday
  - `1_weekly_low_day.png` — LOW distribution by weekday
  - `2_weekly_high_session.png` — HIGH distribution by session
  - `2_weekly_low_session.png` — LOW distribution by session
  - `3_weekly_extreme_hours.png` — Extreme distribution by hour
  - `4_weekly_high_day_session_heatmap.png` — HIGH by day × session
  - `4_weekly_low_day_session_heatmap.png` — LOW by day × session
- 5 experiment charts (hypothesis testing)

### Experiment Framework

The script includes a `run_experiment()` function for custom analysis:

```python
# Example: P(High forms on Friday | Bullish week)
run_experiment(weekly, "Bull_Bear", "High_Weekday", target_order=DAYS)

# Example: Compare bullish/bearish weeks
run_experiment(weekly, "Prev_Bull_Bear", "Bull_Bear")

# Example: Session-based analysis
run_experiment(weekly, "Low_Session", "High_Weekday",
               factor_order=SESSION_ORDER, target_order=DAYS)
```

---

## Key Findings (ES, 1h resample, 2020–2025)

### Dataset Summary
- **273 weeks** analyzed
- **156 bullish** (57%), **117 bearish** (43%)

### Bullish Weeks
- **LOW forms on Monday:** ~59% (buy the dip early week)
- **HIGH forms on Friday:** ~61% (sell into strength end of week)
- Pattern: Strong "buy Monday low, sell Friday high" bias

### Bearish Weeks
- **HIGH forms on Monday:** ~45% (short early weakness)
- **LOW forms on Friday:** ~56% (cover into support)

### Session Patterns
- **Asia session** (19:00–00:00 ET): Often produces weekly extremes during Asia market hours
- **London session** (00:00–09:00 ET): Continuation or reversal patterns
- **NY AM session** (09:00–12:00 ET): High volatility, often establishes weekly direction
- **NY PM session** (12:00–16:00 ET): May close out weekly extremes

---

## Script Structure

### 1. Load & Resample
```python
load_and_resample(path, resample_to)
```
- Loads parquet file
- Localizes ET timezone
- Resamples OHLCV data to specified frequency

### 2. Build Weekly Data
```python
build_weekly(df)
```
- Groups data into Monday-anchored weeks (custom `trading_week_monday()` key)
- Extracts:
  - `Bull_Bear` — Weekly close vs. open (Bullish/Bearish)
  - `Prev_Bull_Bear` — Previous week's direction
  - `Low_Weekday`, `High_Weekday` — Which weekday the extreme formed
  - `Low_Session`, `High_Session` — Which session the extreme formed
  - `Low_Hour`, `High_Hour` — Which hour (for hourly data)

### 3. Generate Standard Charts
- **Day Distribution:** Probability of extreme forming on each day
- **Session Distribution:** Probability of extreme forming in each session
- **Hour Distribution:** Probability of extreme forming in each hour
- **Day×Session Heatmap:** Joint distribution visualization

### 4. Run Experiments
```python
run_experiment(weekly, factor_col, target_col, factor_order=None, target_order=None)
```
- Compares conditional probability P(Y|X) against baseline P(Y)
- Produces side-by-side bar chart
- Highlights significant deviations

---

## Critical Bug Fix

**Issue:** `pd.Grouper(freq="W-MON")` creates Tuesday→Monday buckets, with Monday as the LAST day of each group. This inflates Monday's extremes and leaks Sunday 18:00–23:59 CME open data into wrong groups.

**Solution:** Custom `trading_week_monday()` function:
- Assigns Sunday 18:00+ (CME open) to the **next** Monday
- Creates proper Monday→Friday buckets
- Pairs with `trading_weekday()` for correct weekday labeling (Sunday timestamps → "Monday")

---

## Sessions (ET Time Zones)

| Session | ET Hours | Notes |
|---------|----------|-------|
| Asia | 19:00–00:00 | CME open, overnight |
| London | 00:00–09:00 | European AM |
| NY AM | 09:00–12:00 | US morning open |
| NY PM | 12:00–16:00 | US afternoon |
| Other | 16:00–19:00 | Evening, pre-Asia |

---

## File Structure

```
weekly po3/
├── README.md                    # This file
├── CLAUDE.md                    # Project guidelines and architecture
├── analysis.py                  # Main monolith script
├── data/
│   ├── es_1m.parquet           # E-mini S&P 500 data (1-minute OHLCV)
│   └── nq_1m.parquet           # E-mini Nasdaq-100 data (1-minute OHLCV)
└── output/                      # Generated output (charts and data)
    ├── *.png                    # Standard charts (matplotlib)
    └── *.csv                    # Exported analysis data
```

---

## Requirements

- Python 3.8+
- `pandas` — Data manipulation
- `numpy` — Numerical operations
- `pyarrow` — Parquet file I/O
- `matplotlib` — Chart generation

Install dependencies:
```bash
pip install pandas numpy matplotlib pyarrow
```

---

## Usage Examples

### Run Full Analysis (ES, 1-hour data)
```bash
python analysis.py
```
Generates all default charts and prints weekly summary.

### Customize Data Source
Edit `analysis.py` CONFIG:
```python
SYMBOL = "NQ"
DATA_PATH = "data/nq_1m.parquet"
```

### Change Resample Frequency
```python
RESAMPLE_TO = "4h"  # 4-hour candles instead of 1-hour
```

---

## References

- **Trading Weeks:** Groups are Monday (18:00 CME open Sunday evening) through Friday (16:00 ET close)
- **Timezone:** All timestamps in Eastern Time (America/New_York)
- **Data Period:** 2020–2025 (5+ years)
- **Chart Format:** PNG files (matplotlib) saved at 150 DPI to `output/`

---

## Notes

- The `DateTime_ET` column requires timezone localization before use; the script handles this automatically
- Session labels (Asia, London, etc.) are computed from raw timestamps — they do not need to be pre-loaded in the parquet file
- Charts show relative probability distributions; absolute probabilities depend on sample size
- Backtesting results require proper transaction costs and slippage assumptions

---

**Last Updated:** March 2026
**Data Period:** 2020–2025
