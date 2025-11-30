import csv
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent
IN_PATH = BASE_DIR / "neo4j_huts" / "huts_edges_ors.csv"
OUT_PATH = BASE_DIR / "neo4j_huts" / "huts_edges_ors_max35.csv"

def main():
    with IN_PATH.open(newline="", encoding="utf-8") as f_in, \
         OUT_PATH.open("w", newline="", encoding="utf-8") as f_out:

        reader = csv.DictReader(f_in)
        fieldnames = reader.fieldnames
        writer = csv.DictWriter(f_out, fieldnames=fieldnames)
        writer.writeheader()

        kept = 0
        skipped = 0

        for row in reader:
            val = row.get("distance_km:float", "").strip()
            if not val:
                skipped += 1
                continue

            try:
                d = float(val)
            except ValueError:
                skipped += 1
                continue

            if d <= 35.0:
                writer.writerow(row)
                kept += 1
            else:
                skipped += 1

    print(f"Conservé {kept} arêtes <= 35 km, ignoré {skipped}.")
    print(f"Fichier écrit: {OUT_PATH}")

if __name__ == "__main__":
    main()
