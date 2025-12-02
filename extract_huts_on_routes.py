import json
import csv
from pathlib import Path
from collections import defaultdict

BASE_DIR = Path(__file__).resolve().parent

OVERPASS_JSON = BASE_DIR / "osm_routes" / "laponie_routes.json"
HUTS_CSV = BASE_DIR / "neo4j_huts" / "huts.csv"
OUTPUT_CSV = BASE_DIR / "neo4j_routes" / "huts_on_routes.csv"


def load_node_to_routes():
    """
    Lit le JSON Overpass et construit un mapping:
      node_osm_id -> ensemble de route_ids (relations route=hiking|ski)
    """
    with OVERPASS_JSON.open(encoding="utf-8") as f:
        data = json.load(f)

    elements = data.get("elements", [])
    node_to_routes = defaultdict(set)

    for el in elements:
        if el.get("type") != "relation":
            continue

        tags = el.get("tags", {})
        route_type = tags.get("route")
        if route_type not in ("hiking", "ski"):
            continue

        route_id = el["id"]

        # on parcourt les membres de la relation
        for mem in el.get("members", []):
            if mem.get("type") == "node":
                ref = mem.get("ref")
                if ref is not None:
                    node_to_routes[ref].add(route_id)

    print(f"{sum(len(v) for v in node_to_routes.values())} associations node-route trouvées")
    return node_to_routes


def main():
    node_to_routes = load_node_to_routes()

    OUTPUT_CSV.parent.mkdir(exist_ok=True)

    with HUTS_CSV.open(newline="", encoding="utf-8") as f_in, \
         OUTPUT_CSV.open("w", newline="", encoding="utf-8") as f_out:

        reader = csv.DictReader(f_in)

        fieldnames = [
            ":START_ID(Hut)",
            ":END_ID(Route)",
            "role",
        ]
        writer = csv.DictWriter(f_out, fieldnames=fieldnames)
        writer.writeheader()

        count = 0

        for row in reader:
            # On récupère hut_id (ID du noeud Hut dans Neo4j)
            hut_id_str = row.get("hut_id:ID(Hut)") or row.get("hut_id")
            if not hut_id_str:
                continue
            hut_id = int(hut_id_str)

            # On récupère l'osm_id du node OSM de la cabane
            osm_id_str = row.get("osm_id:long") or row.get("osm_id")
            if not osm_id_str:
                continue
            try:
                osm_id = int(osm_id_str)
            except ValueError:
                continue

            # Pour ce node OSM, quelles routes le référencent comme membre ?
            route_ids = node_to_routes.get(osm_id, set())
            if not route_ids:
                continue

            for route_id in route_ids:
                writer.writerow({
                    ":START_ID(Hut)": hut_id,
                    ":END_ID(Route)": route_id,
                    "role": "member",  # on pourra raffiner plus tard
                })
                count += 1

    print(f"{count} relations Hut-Route écrites dans {OUTPUT_CSV}")


if __name__ == "__main__":
    main()
