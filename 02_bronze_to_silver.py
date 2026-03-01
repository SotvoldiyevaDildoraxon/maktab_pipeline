import pandas as pd
from pathlib import Path

BRONZE_DIR = Path("data/bronze")
SILVER_LONG_DIR = Path("data/silver_long")
SILVER_LONG_DIR.mkdir(parents=True, exist_ok=True)

files = list(BRONZE_DIR.glob("*.csv"))
print("Bronze fayllar:", len(files))

for file in files:
    print("Processing:", file.name)

    # encoding muammo bo‘lsa yechamiz
    try:
        df = pd.read_csv(file)
    except UnicodeDecodeError:
        df = pd.read_csv(file, encoding="cp1251")

    # yil ustunlari (2010, 2011, ... kabi)
    year_cols = [c for c in df.columns if c.isdigit()]
    if not year_cols:
        print("  ⚠️ Yil ustunlari topilmadi, o'tkazib yuborildi.")
        continue

    # region ustuni
    region_col = "Klassifikator" if "Klassifikator" in df.columns else df.columns[1]

    # wide -> long
    df_long = df.melt(
        id_vars=[region_col],
        value_vars=year_cols,
        var_name="year",
        value_name="value"
    ).rename(columns={region_col: "region"})

    # indicator = fayl nomi
    df_long["indicator"] = file.stem

    # year numeric bo‘lsin
    df_long["year"] = pd.to_numeric(df_long["year"], errors="coerce").astype("Int64")

    # value numeric bo‘lsin (bo‘sh joy / vergul muammolari)
    df_long["value"] = (
        df_long["value"]
        .astype(str)
        .str.replace(" ", "", regex=False)
        .str.replace(",", ".", regex=False)
    )
    df_long["value"] = pd.to_numeric(df_long["value"], errors="coerce")

    # saqlash
    out_path = SILVER_LONG_DIR / f"{file.stem}.parquet"
    df_long.to_parquet(out_path, index=False)

print("\n✅ Bronze → Silver_Long (wide→long) yakunlandi. Natija: data/silver_long/*.parquet")
