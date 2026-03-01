from pathlib import Path

BRONZE_DIR = Path("data/bronze")
SILVER_DIR = Path("data/silver")

csv_files = list(BRONZE_DIR.glob("*.csv"))
print("CSV fayllar:", len(csv_files))

def human_size(n: int) -> str:
    # bayt -> KB/MB/GB
    for unit in ["B", "KB", "MB", "GB", "TB"]:
        if n < 1024:
            return f"{n:.2f} {unit}"
        n = n / 1024
    return f"{n:.2f} PB"

total_csv = 0
total_pq = 0

print("\nFAYL NOMI | CSV | PARQUET | QANCHA KICHIK")
print("-" * 70)

for csv in csv_files:
    pq = SILVER_DIR / f"{csv.stem}.parquet"

    csv_size = csv.stat().st_size
    total_csv += csv_size

    if pq.exists():
        pq_size = pq.stat().st_size
        total_pq += pq_size

        ratio = csv_size / pq_size if pq_size > 0 else 0
        print(f"{csv.stem} | {human_size(csv_size)} | {human_size(pq_size)} | {ratio:.1f}x")
    else:
        print(f"{csv.stem} | {human_size(csv_size)} | (yo‘q) | -")

print("-" * 70)
print("Jami CSV:", human_size(total_csv))
print("Jami Parquet:", human_size(total_pq) if total_pq > 0 else "(yo‘q)")
