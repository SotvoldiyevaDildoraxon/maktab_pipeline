import pandas as pd
from pathlib import Path
from datetime import datetime

RAW_DIR = Path("data/raw")
BRONZE_DIR = Path("data/bronze")
BRONZE_DIR.mkdir(parents=True, exist_ok=True)

run_ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

files = list(RAW_DIR.glob("*.csv"))
print("RAW fayllar soni:", len(files))

for file in files:
    print("Ingest:", file.name)

    # encoding muammo bo‘lsa yechamiz
    try:
        df = pd.read_csv(file)
    except UnicodeDecodeError:
        df = pd.read_csv(file, encoding="cp1251")

    # minimal metadata
    df["source_file"] = file.name
    df["ingest_time"] = run_ts

    out_path = BRONZE_DIR / file.name
    df.to_csv(out_path, index=False)

print("\n✅ 01_raw_to_bronze tugadi. Natija: data/bronze/*.csv")
