import pandas as pd
import os

# ── CONFIG ────────────────────────────────────────────────────────────────────
INPUT_CSV = "results.csv"   # your weekly summary with columns: 
                            # Week_Start, Bull_Bear, High_Day, Low_Day
# ─────────────────────────────────────────────────────────────────────────────

# 1. LOAD
df = pd.read_csv(INPUT_CSV)

# 2. OVERALL BULL/BEAR PCT
counts = df['Bull_Bear'].value_counts()
total  = counts.sum()
pct = (counts / total * 100).round(2)
print("Overall distribution of weeks:")
for wb in ['Bullish','Bearish']:
    print(f"  {wb}: {pct.get(wb, 0):.2f}% ({counts.get(wb,0)} weeks)")
print()

# 3. CONDITIONAL PROBS FOR LOW_DAY
days = ['Monday','Tuesday','Wednesday','Thursday','Friday']
low_cond = {}
for wb in ['Bullish','Bearish']:
    sub = df[df['Bull_Bear'] == wb]
    vc  = sub['Low_Day'].value_counts(normalize=True) * 100
    low_cond[wb] = vc.reindex(days, fill_value=0).round(2)

# 4. DISPLAY LOW_DAY TABLE
os.system("cls")
print("Chance the weekly LOW formed on each weekday:")
print()
print(f"{'Week Type':<10} | " + " | ".join(f"{d[:3]:>6}" for d in days))
print("-" * (12 + 8*len(days)))
for wb in ['Bullish','Bearish']:
    row = " | ".join(f"{low_cond[wb][d]:6.2f}%" for d in days)
    print(f"{wb:<10} | {row}")
print()

# 5. OPTIONAL: save low-day distribution
low_df = pd.DataFrame(low_cond).T
low_df.index.name = 'Week_Type'
low_df.to_csv("low_day_distribution_by_week_type.csv")

# 6. CONDITIONAL PROBS FOR HIGH_DAY
high_cond = {}
for wb in ['Bullish','Bearish']:
    sub = df[df['Bull_Bear'] == wb]
    vc  = sub['High_Day'].value_counts(normalize=True) * 100
    high_cond[wb] = vc.reindex(days, fill_value=0).round(2)

# 7. DISPLAY HIGH_DAY TABLE
print("\nChance the weekly HIGH formed on each weekday:")
print()
print(f"{'Week Type':<10} | " + " | ".join(f"{d[:3]:>6}" for d in days))
print("-" * (12 + 8*len(days)))
for wb in ['Bullish','Bearish']:
    row = " | ".join(f"{high_cond[wb][d]:6.2f}%" for d in days)
    print(f"{wb:<10} | {row}")
print()

# 8. OPTIONAL: save high-day distribution
high_df = pd.DataFrame(high_cond).T
high_df.index.name = 'Week_Type'
high_df.to_csv("high_day_distribution_by_week_type.csv")