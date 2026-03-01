import pandas as pd
from pathlib import Path

BRONZE_DIR = Path("data/bronze")
SILVER_DIR = Path("data/silver")
SILVER_DIR.mkdir(parents=True, exist_ok=True)

files = list(BRONZE_DIR.glob("*.csv"))
print("Bronze fayllar soni:", len(files))

for file in files:
    print("Convert:", file.name)

    try:
        df = pd.read_csv(file)
    except UnicodeDecodeError:
        df = pd.read_csv(file, encoding="cp1251")

    out_path = SILVER_DIR / f"{file.stem}.parquet"
    df.to_parquet(out_path, index=False)

print("\n✅ 02_csv_to_parquet tugadi. Natija: data/silver/*.parquet")
