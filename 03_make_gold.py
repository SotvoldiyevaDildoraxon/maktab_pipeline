import pandas as pd
from pathlib import Path

IN_FILE = Path("data/silver_long_all.parquet")
GOLD_DIR = Path("data/gold")
GOLD_DIR.mkdir(parents=True, exist_ok=True)

OUT_FILE = GOLD_DIR / "kpi_region_year.parquet"

# 1) Long datasetni o‘qish
df = pd.read_parquet(IN_FILE)

# 2) Tekshiruv
need = {"region", "year", "value", "indicator"}
missing = need - set(df.columns)
if missing:
    raise ValueError(f"Kerakli ustunlar yetishmayapti: {missing}")

# 3) GOLD: pivot (indicator -> ustun)
gold = df.pivot_table(
    index=["region", "year"],
    columns="indicator",
    values="value",
    aggfunc="sum"
).reset_index()

# 4) Saqlash
gold.to_parquet(OUT_FILE, index=False)

print("✅ GOLD tayyor:", OUT_FILE)
print("Shape:", gold.shape)
print(gold.head(20))
