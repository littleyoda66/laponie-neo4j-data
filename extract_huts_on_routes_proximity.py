import json
import csv
import math
from pathlib import Path
from collections import defaultdict

BASE_DIR = Path(__file__).resolve().parent

OVERPASS_JSON = BASE_DIR / "osm_routes" / "laponie_routes.json"
HUTS_CSV = BASE_DIR / "neo4j_huts" / "huts.csv"
OUTPUT_CSV = BASE_DIR / "neo4j_routes" / "huts_on_routes_proximity.csv"

# Seuil max pour considérer qu'une hut est "sur" la route
THRESHOLD_METERS = 500.0

EARTH_RADIUS_M = 6_371_000.0


def point_segment_distance_m(lat, lon, lat1, lon1, lat2, lon2):
    """
    Distance d'un point (lat, lon) à un segment [ (lat1,lon1) - (lat2,lon2) ] en mètres.
    Approximation equirectangulaire suffisante à cette échelle.
    """
    # conversion en radians
    lat_rad = math.radians(lat)
    lon_rad = math.radians(lon)
    lat1_rad = math.radians(lat1)
    lon1_rad = math.radians(lon1)
    lat2_rad = math.radians(lat2)
    lon2_rad = math.radians(lon2)

    # latitude moyenne pour corriger la convergence des méridiens
    lat0_rad = (lat1_rad + lat2_rad) / 2.0

    # origine au point 1
    x1, y1 = 0.0, 0.0
    x2 = (lon2_rad - lon1_rad) * math.cos(lat0_rad) * EARTH_RADIUS_M
    y2 = (lat2_rad - lat1_rad) * EARTH_RADIUS_M
    xp = (lon_rad - lon1_rad) * math.cos(lat0_rad) * EARTH_RADIUS_M
    yp = (lat_rad - lat1_rad) * EARTH_RADIUS_M

    dx = x2 - x1
    dy = y2 - y1
    seg_len2 = dx * dx + dy * dy

    if seg_len2 == 0.0:
        # segment dégénéré : distance au point 1
        return math.hypot(xp - x1, yp - y1)

    # projection du point sur la droite paramétrée par t
    t = ((xp - x1) * dx + (yp - y1) * dy) / seg_len2

    if t <= 0.0:
        xn, yn = x1, y1
    elif t >= 1.0:
        xn, yn = x2, y2
    else:
        xn = x1 + t * dx
        yn = y1 + t * dy

    return math.hypot(xp - xn, yp - yn)


def load_osm_graph():
    """
    Charge le JSON Overpass et construit:
      - nodes_by_id:  node_id -> (lat, lon)
      - ways_by_id:   way_id -> [node_ids]
      - routes:       route_id -> { 'name', 'route', 'way_ids': [...] }
    """
    with OVERPASS_JSON.open(encoding="utf-8") as f:
        data = json.load(f)

    elements = data.get("elements", [])

    nodes_by_id = {}
    ways_by_id = {}
    relations = []

    for el in elements:
        if el.get("type") == "node":
            nodes_by_id[el["id"]] = (el["lat"], el["lon"])
        elif el.get("type") == "way":
            ways_by_id[el["id"]] = el.get("nodes", [])
        elif el.get("type") == "relation":
            relations.append(el)

    routes = {}

    for rel in relations:
        tags = rel.get("tags", {})
        route_type = tags.get("route")
        if route_type not in ("hiking", "ski"):
            continue

        route_id = rel["id"]
        way_ids = [
            mem["ref"]
            for mem in rel.get("members", [])
            if mem.get("type") == "way"
        ]

        # Nettoyage / dédoublonnage
        way_ids = list(dict.fromkeys(way_ids))

        routes[route_id] = {
            "name": tags.get("name", ""),
            "route": route_type,
            "way_ids": way_ids,
        }

    print(f"{len(nodes_by_id)} nodes OSM chargés")
    print(f"{len(ways_by_id)} ways OSM chargées")
    print(f"{len(routes)} routes (relations hiking/ski) chargées")

    return nodes_by_id, ways_by_id, routes


def load_huts():
    huts = []
    with HUTS_CSV.open(newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            hut_id_str = row.get("hut_id:ID(Hut)") or row.get("hut_id")
            if not hut_id_str:
                continue
            try:
                hut_id = int(hut_id_str)
            except ValueError:
                continue

            try:
                lat = float(row["latitude:float"])
                lon = float(row["longitude:float"])
            except (KeyError, ValueError):
                continue

            name = row.get("name", f"Hut #{hut_id}")
            huts.append(
                {
                    "hut_id": hut_id,
                    "lat": lat,
                    "lon": lon,
                    "name": name,
                }
            )

    print(f"{len(huts)} huts chargées depuis {HUTS_CSV}")
    return huts


def min_distance_hut_to_route(hut, route, ways_by_id, nodes_by_id):
    """
    Calcule la distance minimale (en m) entre la hut et la polyligne de la route
    (toutes les ways de la relation).
    Retourne None si aucune géométrie utilisable.
    """
    lat = hut["lat"]
    lon = hut["lon"]
    min_dist = None

    for way_id in route["way_ids"]:
        node_ids = ways_by_id.get(way_id)
        if not node_ids or len(node_ids) < 2:
            continue

        # on parcourt les segments de la polyline
        for i in range(len(node_ids) - 1):
            nid1 = node_ids[i]
            nid2 = node_ids[i + 1]

            p1 = nodes_by_id.get(nid1)
            p2 = nodes_by_id.get(nid2)
            if p1 is None or p2 is None:
                continue

            lat1, lon1 = p1
            lat2, lon2 = p2

            d = point_segment_distance_m(lat, lon, lat1, lon1, lat2, lon2)
            if min_dist is None or (d < min_dist):
                min_dist = d

    return min_dist


def main():
    nodes_by_id, ways_by_id, routes = load_osm_graph()
    huts = load_huts()

    OUTPUT_CSV.parent.mkdir(exist_ok=True)

    total_pairs = 0
    kept_pairs = 0

    with OUTPUT_CSV.open("w", newline="", encoding="utf-8") as f_out:
        fieldnames = [
            ":START_ID(Hut)",
            ":END_ID(Route)",
            "near_distance_m:float",
        ]
        writer = csv.DictWriter(f_out, fieldnames=fieldnames)
        writer.writeheader()

        for hut in huts:
            hut_id = hut["hut_id"]
            name = hut["name"]
            lat = hut["lat"]
            lon = hut["lon"]

            print(f"Hut {hut_id} – {name}")

            for route_id, route in routes.items():
                total_pairs += 1

                dist = min_distance_hut_to_route(hut, route, ways_by_id, nodes_by_id)
                if dist is None:
                    continue

                if dist <= THRESHOLD_METERS:
                    kept_pairs += 1
                    writer.writerow(
                        {
                            ":START_ID(Hut)": hut_id,
                            ":END_ID(Route)": route_id,
                            "near_distance_m:float": f"{dist:.2f}",
                        }
                    )
                    print(
                        f"  -> proche de route {route_id} "
                        f"({route['name'] or route['route']}) : {dist:.1f} m"
                    )

    print(f"\nTotal hut-route pairs examinés : {total_pairs}")
    print(f"Paires retenues (<= {THRESHOLD_METERS} m) : {kept_pairs}")
    print(f"CSV écrit dans {OUTPUT_CSV}")


if __name__ == "__main__":
    main()
