import json
import math
from pathlib import Path
from collections import defaultdict
import csv
import heapq


# -----------------------------
# Distance Haversine (mètres)
# -----------------------------
def haversine(lat1, lon1, lat2, lon2):
    R = 6_371_000  # metres
    phi1 = math.radians(lat1)
    phi2 = math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlambda = math.radians(lon2 - lon1)

    a = (
        math.sin(dphi / 2) ** 2
        + math.cos(phi1) * math.cos(phi2) * math.sin(dlambda / 2) ** 2
    )
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
    return R * c


# -----------------------------
# Chargement des chemins OSM
# -----------------------------
def load_nordics_paths(path: Path):
    print(f"Lecture du fichier principal (paths) : {path}")
    data = json.loads(path.read_text(encoding="utf-8"))
    elements = data.get("elements", [])

    nodes = {}
    ways_by_id = {}

    for el in elements:
        etype = el.get("type")
        if etype == "node":
            nodes[el["id"]] = el
        elif etype == "way":
            ways_by_id[el["id"]] = el

    print(f"  Nodes (chemins) : {len(nodes)}")
    print(f"  Ways  (chemins) : {len(ways_by_id)}")
    return nodes, ways_by_id


# -----------------------------
# IDs de huts à exclure (optionnel)
# -----------------------------
def load_excluded_hut_ids(path: Path):
    excluded = set()
    if not path.exists():
        print("Pas de fichier excluded_huts.txt, aucune hut exclue explicitement.")
        return excluded

    print(f"Lecture de la liste d'exclusions: {path}")
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        try:
            excluded.add(int(line))
        except ValueError:
            print(f"  Ligne ignorée (pas un entier): {line}")
    print(f"  {len(excluded)} hut_ids exclus explicitement.")
    return excluded


# -----------------------------
# Chargement des huts par pays
# -----------------------------
def load_huts_per_country(hut_sources, nodes, excluded_ids=None):
    """
    hut_sources : liste de (fichier_json, country_code)

    On garde :
      - node/way/relation avec centre géométrique,
      - avec un 'name' non vide,
      - pas dans excluded_ids.
    On stocke tous les tags OSM dans hut_meta["tags"].
    """
    if excluded_ids is None:
        excluded_ids = set()

    hut_ids = set()
    hut_meta = {}

    for path, cc in hut_sources:
        if not path.exists():
            print(f"ATTENTION: fichier huts {path} introuvable pour {cc}, on ignore.")
            continue

        print(f"Lecture huts {cc} : {path}")
        data = json.loads(path.read_text(encoding="utf-8"))
        elements = data.get("elements", [])

        count_elements = 0
        kept_as_hut = 0

        for el in elements:
            etype = el.get("type")
            if etype not in ("node", "way", "relation"):
                continue

            node_id = el["id"]
            count_elements += 1

            # Coordonnées
            if etype == "node":
                lat = el.get("lat")
                lon = el.get("lon")
            else:
                center = el.get("center")
                if not center:
                    continue
                lat = center.get("lat")
                lon = center.get("lon")

            if lat is None or lon is None:
                continue

            tags = el.get("tags", {}) or {}
            name = tags.get("name", "")
            if not name or not name.strip():
                # on vire les objets sans nom
                continue

            if node_id in excluded_ids:
                continue

            tourism = tags.get("tourism")
            amenity = tags.get("amenity")
            shelter_type = tags.get("shelter_type")
            operator = tags.get("operator", "") or ""

            # S'assurer que nodes[node_id] a lat/lon/tags
            if node_id not in nodes:
                nodes[node_id] = {
                    "id": node_id,
                    "lat": lat,
                    "lon": lon,
                    "tags": tags,
                }
            else:
                nodes[node_id]["lat"] = nodes[node_id].get("lat", lat)
                nodes[node_id]["lon"] = nodes[node_id].get("lon", lon)

            hut_ids.add(node_id)
            hut_meta[node_id] = {
                "osm_id":       node_id,
                "name":         name,
                "country_code": cc,
                "tourism":      tourism,
                "amenity":      amenity,
                "shelter_type": shelter_type,
                "operator":     operator,
                "tags":         tags,
            }
            kept_as_hut += 1

        print(f"  Éléments total (nodes/ways/relations) pour {cc}: {count_elements}")
        print(f"  Huts gardées pour {cc}: {kept_as_hut}")

    print(f"Total de Huts distinctes (avant ancrage): {len(hut_ids)}")
    return hut_ids, hut_meta


# -----------------------------
# Graphe des chemins
# -----------------------------
def build_graph(nodes, ways_by_id):
    graph = defaultdict(list)

    print("Construction du graphe (adjacence chemins)...")
    edge_count = 0

    for way_id, way in ways_by_id.items():
        node_ids = way.get("nodes", [])
        if len(node_ids) < 2:
            continue

        for i in range(len(node_ids) - 1):
            n1 = node_ids[i]
            n2 = node_ids[i + 1]

            if n1 not in nodes or n2 not in nodes:
                continue

            lat1, lon1 = nodes[n1]["lat"], nodes[n1]["lon"]
            lat2, lon2 = nodes[n2]["lat"], nodes[n2]["lon"]
            dist = haversine(lat1, lon1, lat2, lon2)

            graph[n1].append((n2, dist))
            graph[n2].append((n1, dist))
            edge_count += 2

    print(f"  Nombre d'arêtes (x2): {edge_count}")
    print(f"  Nombre de nœuds dans le graphe: {len(graph)}")
    return graph


# -----------------------------
# Index spatial (pour ancrage huts->chemins)
# -----------------------------
def build_spatial_index(nodes, graph, cell_size_deg=0.05):
    cells = defaultdict(list)
    for node_id in graph.keys():
        node = nodes[node_id]
        lat = node["lat"]
        lon = node["lon"]
        i = int(lat / cell_size_deg)
        j = int(lon / cell_size_deg)
        cells[(i, j)].append(node_id)
    print(f"Index spatial : {len(cells)} cellules")
    return cells


def find_nearest_graph_node_for_hut(hut_id, nodes, graph, cells,
                                    max_radius_m=3_000.0, cell_size_deg=0.05):
    """
    Renvoie l'ID du noeud de chemin le plus proche de la hut,
    à moins de max_radius_m, ou None.
    """
    if hut_id in graph:
        return hut_id

    hut = nodes[hut_id]
    lat = hut["lat"]
    lon = hut["lon"]

    i0 = int(lat / cell_size_deg)
    j0 = int(lon / cell_size_deg)

    best_node = None
    best_dist = max_radius_m + 1.0

    for di in (-1, 0, 1):
        for dj in (-1, 0, 1):
            key = (i0 + di, j0 + dj)
            for nid in cells.get(key, []):
                node = nodes[nid]
                d = haversine(lat, lon, node["lat"], node["lon"])
                if d < best_dist:
                    best_dist = d
                    best_node = nid

    if best_node is not None and best_dist <= max_radius_m:
        return best_node
    return None


def compute_hut_anchors(nodes, graph, hut_ids, max_radius_m=3_000.0):
    print("Construction index spatial pour les ancrages huts->chemins...")
    cell_size_deg = 0.05
    cells = build_spatial_index(nodes, graph, cell_size_deg=cell_size_deg)

    anchor_by_hut = {}
    huts_by_anchor = defaultdict(list)
    no_anchor = 0

    hut_ids_list = list(hut_ids)

    for idx, hut_id in enumerate(hut_ids_list, start=1):
        if idx % 50 == 0 or idx == 1:
            print(f"  Ancrage hut {idx}/{len(hut_ids_list)} (osm_id={hut_id})")

        anchor = find_nearest_graph_node_for_hut(
            hut_id, nodes, graph, cells,
            max_radius_m=max_radius_m,
            cell_size_deg=cell_size_deg
        )
        if anchor is None:
            no_anchor += 1
            continue

        anchor_by_hut[hut_id] = anchor
        huts_by_anchor[anchor].append(hut_id)

    print(f"Huts avec ancrage: {len(anchor_by_hut)}")
    print(f"Huts sans ancrage (> {max_radius_m} m d'un chemin): {no_anchor}")
    return anchor_by_hut, huts_by_anchor


# -----------------------------
# Filtrage des liaisons indirectes
# -----------------------------
def prune_redundant_edges(best_dist_for_pair, epsilon=0.05):
    """
    Supprime les liaisons A-B pour lesquelles il existe une hut C telle que :
      d(A,C) + d(C,B) <= d(A,B) * (1 + epsilon)
    """
    huts = set()
    for (a, b) in best_dist_for_pair.keys():
        huts.add(a)
        huts.add(b)

    def dist(a, b):
        if a == b:
            return 0.0
        key = (a, b) if a < b else (b, a)
        return best_dist_for_pair.get(key)

    to_remove = set()

    pairs_items = list(best_dist_for_pair.items())
    print(f"Filtrage des liaisons avec hut intermédiaire (pairs={len(pairs_items)})...")

    for (a, b), d_ab in pairs_items:
        if (a, b) in to_remove:
            continue
        for c in huts:
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

    print(f"  Liaisons supprimées (indirectes): {len(to_remove)}")

    for key in to_remove:
        if key in best_dist_for_pair:
            del best_dist_for_pair[key]


# -----------------------------
# Graphe Hut <-> Hut
# -----------------------------
def build_hut_graph(nodes, graph, hut_ids, hut_meta,
                    anchor_by_hut, huts_by_anchor,
                    max_distance_km=40.0):
    max_distance_m = max_distance_km * 1000.0

    hut_ids_with_anchor = sorted(anchor_by_hut.keys())
    print(f"Nombre de huts avec ancrage dans le graphe: {len(hut_ids_with_anchor)}")

    best_dist_for_pair = {}

    for idx, hut_source in enumerate(hut_ids_with_anchor, start=1):
        anchor_src = anchor_by_hut[hut_source]
        if idx % 10 == 0 or idx == 1:
            print(f"  Dijkstra hut {idx}/{len(hut_ids_with_anchor)} "
                  f"(hut osm_id={hut_source}, anchor={anchor_src})")

        dist_dict = {anchor_src: 0.0}
        heap = [(0.0, anchor_src)]

        while heap:
            d, node = heapq.heappop(heap)
            if d > max_distance_m:
                break
            if d != dist_dict.get(node, float("inf")):
                continue

            # Si ce noeud est l'ancrage d'une ou plusieurs huts (autres que la source)
            if node in huts_by_anchor and node != anchor_src:
                d_km = d / 1000.0
                for hut_target in huts_by_anchor[node]:
                    if hut_target == hut_source:
                        continue
                    a = min(hut_source, hut_target)
                    b = max(hut_source, hut_target)
                    old = best_dist_for_pair.get((a, b))
                    if old is None or d_km < old:
                        best_dist_for_pair[(a, b)] = d_km

                # On ne propage pas au-delà : on ne veut pas "sauter" des huts
                continue

            for neigh, w in graph.get(node, []):
                nd = d + w
                if nd < dist_dict.get(neigh, float("inf")) and nd <= max_distance_m:
                    dist_dict[neigh] = nd
                    heapq.heappush(heap, (nd, neigh))

    print(f"  Paires hut-hut brutes avant filtrage: {len(best_dist_for_pair)}")

    prune_redundant_edges(best_dist_for_pair, epsilon=0.05)

    edges = []
    for (a, b), d_km in best_dist_for_pair.items():
        edges.append((a, b, d_km))

    print(f"Nombre de liens hut-hut après filtrage: {len(edges)}")
    return hut_ids_with_anchor, edges


# -----------------------------
# Écriture des CSV
# -----------------------------
def write_hut_csv(nodes, hut_ids, hut_meta, edges, output_dir: Path):
    output_dir.mkdir(exist_ok=True)

    huts_csv = output_dir / "huts.csv"
    edges_csv = output_dir / "huts_edges.csv"

    hut_fields = [
        "hut_id:ID(Hut)",
        "osm_id:long",
        "latitude:float",
        "longitude:float",
        "name",
        "country_code",
        "tourism",
        "amenity",
        "shelter_type",
        "operator",
        "tags_json",
    ]

    print(f"Écriture {huts_csv}")
    with huts_csv.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=hut_fields)
        writer.writeheader()

        for osm_id in sorted(hut_ids):
            node = nodes[osm_id]
            meta = hut_meta.get(osm_id, {})
            tags = meta.get("tags", {}) or {}
            row = {
                "hut_id:ID(Hut)": osm_id,
                "osm_id:long": osm_id,
                "latitude:float": node["lat"],
                "longitude:float": node["lon"],
                "name": meta.get("name", ""),
                "country_code": meta.get("country_code", ""),
                "tourism": meta.get("tourism", ""),
                "amenity": meta.get("amenity", ""),
                "shelter_type": meta.get("shelter_type", ""),
                "operator": meta.get("operator", ""),
                "tags_json": json.dumps(tags, ensure_ascii=False),
            }
            writer.writerow(row)

    edge_fields = [
        ":START_ID(Hut)",
        ":END_ID(Hut)",
        "distance_km:float",
    ]

    print(f"Écriture {edges_csv}")
    with edges_csv.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=edge_fields)
        writer.writeheader()

        for start_id, end_id, dist_km in edges:
            row = {
                ":START_ID(Hut)": start_id,
                ":END_ID(Hut)": end_id,
                "distance_km:float": dist_km,
            }
            writer.writerow(row)

    print("CSV Huts générés.")


# -----------------------------
# MAIN
# -----------------------------
def main():
    base_dir = Path(".")

    paths_file = base_dir / "overpass_nordics_paths.json"
    hut_sources = [
        (base_dir / "overpass_sweden_huts.json", "SE"),
        (base_dir / "overpass_norway_huts.json", "NO"),
        # Finlande ignorée pour l'instant
    ]

    excluded_file = base_dir / "excluded_huts.txt"
    excluded_ids = load_excluded_hut_ids(excluded_file)

    nodes, ways_by_id = load_nordics_paths(paths_file)
    hut_ids, hut_meta = load_huts_per_country(hut_sources, nodes, excluded_ids)
    graph = build_graph(nodes, ways_by_id)

    anchor_by_hut, huts_by_anchor = compute_hut_anchors(
        nodes, graph, hut_ids, max_radius_m=3_000.0
    )

    # Debug : lister les huts sans ancrage
    unanchored = sorted(hut_ids - anchor_by_hut.keys())
    print(f"Huts sans ancrage (n={len(unanchored)}):")
    for hid in unanchored:
        meta = hut_meta.get(hid, {})
        print(f"  {hid} - {meta.get('name', '?')} ({meta.get('country_code', '')})")

    MAX_DISTANCE_KM = 40.0
    huts_with_anchor, edges = build_hut_graph(
        nodes, graph, hut_ids, hut_meta,
        anchor_by_hut, huts_by_anchor,
        max_distance_km=MAX_DISTANCE_KM
    )

    output_dir = base_dir / "neo4j_huts"
    write_hut_csv(nodes, hut_ids, hut_meta, edges, output_dir)


if __name__ == "__main__":
    main()
