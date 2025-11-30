import csv
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent
IN_PATH = BASE_DIR / "neo4j_huts" / "huts_edges_ors.csv"
OUT_PATH = BASE_DIR / "neo4j_huts" / "huts_edges_ors_max35.csv"


def load_edges_max35():
    edges = []  # (a, b, dist_km, dplus, dminus)
    with IN_PATH.open(newline="", encoding="utf-8") as f_in:
        reader = csv.DictReader(f_in)
        for row in reader:
            val = (row.get("distance_km:float") or "").strip()
            if not val:
                continue
            try:
                d = float(val)
            except ValueError:
                continue
            if d > 35.0:
                continue

            try:
                a = int(row[":START_ID(Hut)"])
                b = int(row[":END_ID(Hut)"])
            except ValueError:
                continue

            dplus = row.get("dplus_m:float") or ""
            dminus = row.get("dminus_m:float") or ""
            edges.append((a, b, d, dplus, dminus))
    return edges


def prune_indirect_edges(edges, epsilon=0.05):
    """
    edges : liste (a, b, d_km, dplus, dminus) pour un seul sens (une ligne par paire)

    On construit un dictionnaire non orienté (min(a,b), max(a,b)) -> dist_km
    Puis on supprime les paires A-B pour lesquelles il existe une hut C telle que :
      d(A,C) + d(C,B) <= d(A,B) * (1 + epsilon)
    """
    pair_dist = {}  # (min_id, max_id) -> dist_km
    huts = set()

    for a, b, d, _, _ in edges:
        huts.add(a)
        huts.add(b)
        key = (a, b) if a < b else (b, a)
        old = pair_dist.get(key)
        if old is None or d < old:
            pair_dist[key] = d

    def dist(u, v):
        if u == v:
            return 0.0
        key = (u, v) if u < v else (v, u)
        return pair_dist.get(key)

    to_remove = set()

    print(f"Prune: on a {len(pair_dist)} paires hut-hut avant filtrage...")

    huts_list = list(huts)
    for (a, b), d_ab in list(pair_dist.items()):
        for c in huts_list:
            if c == a or c == b:
                continue
            d_ac = dist(a, c)
            if d_ac is None:
                continue
            d_cb = dist(c, b)
            if d_cb is None:
                continue
            if d_ac + d_cb <= d_ab * (1.0 + epsilon):
                to_remove.add((a, b))
                break

    print(f"  Paires supprimées (indirectes): {len(to_remove)}")

    kept_pairs = set(pair_dist.keys()) - {
        (a, b) if a < b else (b, a) for (a, b) in to_remove
    }
    return kept_pairs


def main():
    edges = load_edges_max35()
    print(f"{len(edges)} arêtes <= 35 km chargées depuis {IN_PATH}")

    kept_pairs = prune_indirect_edges(edges, epsilon=0.05)

    with OUT_PATH.open("w", newline="", encoding="utf-8") as f_out:
        fieldnames = [
            ":START_ID(Hut)",
            ":END_ID(Hut)",
            "distance_km:float",
            "dplus_m:float",
            "dminus_m:float",
        ]
        writer = csv.DictWriter(f_out, fieldnames=fieldnames)
        writer.writeheader()

        kept = 0
        skipped = 0

        for a, b, d, dplus, dminus in edges:
            key = (a, b) if a < b else (b, a)
            if key not in kept_pairs:
                skipped += 1
                continue

            writer.writerow({
                ":START_ID(Hut)": a,
                ":END_ID(Hut)": b,
                "distance_km:float": d,
                "dplus_m:float": dplus,
                "dminus_m:float": dminus,
            })
            kept += 1

    print(f"Conservé {kept} arêtes, supprimé {skipped}.")
    print(f"Fichier écrit: {OUT_PATH}")


if __name__ == "__main__":
    main()
