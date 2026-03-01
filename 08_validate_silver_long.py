import pandas as pd
from pathlib import Path
from datetime import datetime
from logging_setup import get_logger

logger = get_logger("validate")
REPORT_DIR = Path("reports")
REPORT_DIR.mkdir(exist_ok=True)

# Silver_long faylni topish (2 xil joyni tekshiradi)
DATA_PATH = Path("data/silver_long_all.parquet")
if not DATA_PATH.exists():
    DATA_PATH = Path("data/silver_long/silver_long_all.parquet")

df = pd.read_parquet(DATA_PATH)

errors = []
metrics = {}

# Monitoring metrics
metrics["rows"] = len(df)
metrics["cols"] = df.shape[1]
logger.info(f"Read: {DATA_PATH} | rows={metrics['rows']} cols={metrics['cols']}")

# Validation rules
if metrics["rows"] == 0:
    errors.append("Row count = 0")

if "region" not in df.columns or df["region"].isna().any():
    errors.append("Region is NULL (or missing column)")

if "year" not in df.columns:
    errors.append("Missing year column")
else:
    bad_year = df.loc[(df["year"] < 2010) | (df["year"] > 2024), "year"]
    if len(bad_year) > 0:
        errors.append("Year out of range (2010–2024)")

if "value" not in df.columns:
    errors.append("Missing value column")
else:
    neg = df.loc[df["value"] < 0, "value"]
    if len(neg) > 0:
        errors.append(f"Negative values found: {len(neg)} rows")

# Duplicate check: (region, year, indicator)
key_cols = [c for c in ["region", "year", "indicator"] if c in df.columns]
if len(key_cols) == 3:
    dup = df.duplicated(subset=key_cols).sum()
    metrics["duplicates"] = int(dup)
    if dup > 0:
        errors.append(f"Duplicates found: {dup}")

# Report yozish
ts = datetime.now().strftime("%Y%m%d_%H%M%S")
report_path = REPORT_DIR / f"validation_report_{ts}.txt"

with open(report_path, "w", encoding="utf-8") as f:
    f.write("VALIDATION REPORT\n")
    f.write(f"DATA: {DATA_PATH}\n\n")
    f.write("METRICS:\n")
    for k, v in metrics.items():
        f.write(f"- {k}: {v}\n")
    f.write("\nERRORS:\n")
    if errors:
        for e in errors:
            f.write(f"- {e}\n")
    else:
        f.write("- NONE\n")

# Natija
if errors:
    logger.error(f"Validation FAILED. Report: {report_path}")
    raise SystemExit(f"Validation failed. See {report_path}")
else:
    logger.info(f"Validation PASSED. Report: {report_path}")
    print("OK: Validation passed")
