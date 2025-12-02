import json
import csv
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent
OVERPASS_JSON = BASE_DIR / "osm_routes" / "laponie_routes.json"
ROUTES_CSV = BASE_DIR / "neo4j_routes" / "routes.csv"


def main():
    ROUTES_CSV.parent.mkdir(exist_ok=True)

    with OVERPASS_JSON.open(encoding="utf-8") as f:
        data = json.load(f)

    elements = data.get("elements", [])
    routes = []
    seen_ids = set()

    for el in elements:
        if el.get("type") != "relation":
            continue

        tags = el.get("tags", {})
        route_type = tags.get("route")

        # On garde uniquement les relations de rando / ski
        if route_type not in ("hiking", "ski"):
            continue

        rel_id = el["id"]
        if rel_id in seen_ids:
            continue
        seen_ids.add(rel_id)

        routes.append(
            {
                "route_osm_id:ID(Route)": rel_id,
                "name": tags.get("name", ""),
                "route": route_type,
                "network": tags.get("network", ""),
                "ref": tags.get("ref", ""),
                "operator": tags.get("operator", ""),
                "osmc_symbol": tags.get("osmc:symbol", ""),
                "colour": tags.get("colour", ""),
            }
        )

    with ROUTES_CSV.open("w", newline="", encoding="utf-8") as f_out:
        fieldnames = [
            "route_osm_id:ID(Route)",
            "name",
            "route",
            "network",
            "ref",
            "operator",
            "osmc_symbol",
            "colour",
        ]
        writer = csv.DictWriter(f_out, fieldnames=fieldnames)
        writer.writeheader()
        for r in routes:
            writer.writerow(r)

    print(f"{len(routes)} routes écrites dans {ROUTES_CSV}")


if __name__ == "__main__":
    main()
