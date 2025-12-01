import csv
import time
import json
from pathlib import Path
import os

import requests


# -------------------------------------------------------------------
# CONFIG
# -------------------------------------------------------------------

# Clé ORS dans les variables d'environnement
ORS_API_KEY = os.environ.get("ORS_API_KEY")
if not ORS_API_KEY:
    raise RuntimeError(
        "Variable d'environnement ORS_API_KEY non définie. "
        "Définis-la dans les paramètres système ou dans la session avant de lancer le script."
    )

# Profil rando à pied
ORS_URL = "https://api.openrouteservice.org/v2/directions/foot-hiking"

# Répertoire racine du repo
BASE_DIR = Path(__file__).resolve().parent

# Fichiers d'entrée/sortie
HUTS_CSV = BASE_DIR / "neo4j_huts" / "huts.csv"
EDGES_IN_CSV = BASE_DIR / "neo4j_huts" / "huts_edges.csv"
EDGES_OUT_CSV = BASE_DIR / "neo4j_huts" / "huts_edges_ors.csv"

# Pause entre requêtes ORS (free tier)
SLEEP_BETWEEN_CALLS = 2.0  # secondes


# -------------------------------------------------------------------
# Chargement des huts
# -------------------------------------------------------------------
def load_huts(path: Path):
    huts = {}
    with path.open(newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            hut_id = int(row["hut_id:ID(Hut)"])
            huts[hut_id] = {
                "lat": float(row["latitude:float"]),
                "lon": float(row["longitude:float"]),
                "name": row["name"],
            }
    print(f"{len(huts)} huts chargées depuis {path}")
    return huts


# -------------------------------------------------------------------
# Chargement des arêtes hut-hut
# -------------------------------------------------------------------
def load_edges(path: Path):
    edges = []
    with path.open(newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            a = int(row[":START_ID(Hut)"])
            b = int(row[":END_ID(Hut)"])
            edges.append((a, b))
    print(f"{len(edges)} liens hut-hut chargés depuis {path}")
    return edges


# -------------------------------------------------------------------
# Appel à openrouteservice pour une paire de huts
# -------------------------------------------------------------------
def call_ors(hut_a, hut_b):
    """
    hut_a / hut_b : dict avec lat, lon, name
    Retourne (distance_km, dplus_m, dminus_m) ou None en cas d'erreur.
    Gère les deux formats possibles de réponse ORS :
      - GeoJSON: { "features": [ { "properties": { "summary": ... } } ] }
      - JSON:    { "routes":   [ { "summary": ... } ] }
    """
    body = {
        "coordinates": [
            [hut_a["lon"], hut_a["lat"]],
            [hut_b["lon"], hut_b["lat"]],
        ],
        "elevation": True,
    }

    headers = {
        "Authorization": ORS_API_KEY,
        "Content-Type": "application/json",
    }

    try:
        resp = requests.post(ORS_URL, headers=headers, json=body, timeout=30)
    except Exception as e:
        print(f"  ERREUR réseau ORS pour {hut_a['name']} -> {hut_b['name']}: {e}")
        return None

    if resp.status_code != 200:
        print(f"  ERREUR ORS {resp.status_code} pour {hut_a['name']} -> {hut_b['name']}")
        try:
            print("   ", resp.text[:300], "...")
        except Exception:
            pass
        return None

    try:
        data = resp.json()
    except Exception as e:
        print(f"  ERREUR JSON ORS pour {hut_a['name']} -> {hut_b['name']}: {e}")
        try:
            print("   Réponse brute:", resp.text[:300], "...")
        except Exception:
            pass
        return None

    summary = None

    # Format GeoJSON (features)
    if isinstance(data, dict) and "features" in data:
        try:
            feature = data["features"][0]
            props = feature.get("properties", {})
            summary = props.get("summary", {})
        except (KeyError, IndexError, TypeError) as e:
            print(f"  ERREUR parsing 'features' pour {hut_a['name']} -> {hut_b['name']}: {e}")
            try:
                print("   ", json.dumps(data, indent=2)[:400], "...")
            except Exception:
                pass
            return None

    # Format JSON classique (routes)
    elif isinstance(data, dict) and "routes" in data:
        try:
            route = data["routes"][0]
            summary = route.get("summary", {})
        except (KeyError, IndexError, TypeError) as e:
            print(f"  ERREUR parsing 'routes' pour {hut_a['name']} -> {hut_b['name']}: {e}")
            try:
                print("   ", json.dumps(data, indent=2)[:400], "...")
            except Exception:
                pass
            return None

    else:
        print(f"  Réponse ORS inattendue pour {hut_a['name']} -> {hut_b['name']}: ni 'features' ni 'routes'")
        try:
            print("   ", json.dumps(data, indent=2)[:400], "...")
        except Exception:
            pass
        return None

    try:
        distance_m = float(summary["distance"])
        ascent = float(summary.get("ascent", 0.0))
        descent = float(summary.get("descent", 0.0))
    except (KeyError, ValueError, TypeError) as e:
        print(f"  ERREUR lecture summary pour {hut_a['name']} -> {hut_b['name']}: {e}")
        try:
            print("   ", json.dumps(summary, indent=2)[:400], "...")
        except Exception:
            pass
        return None

    distance_km = distance_m / 1_000.0
    return distance_km, ascent, descent


# -------------------------------------------------------------------
# Programme principal
# -------------------------------------------------------------------
def main():
    huts = load_huts(HUTS_CSV)
    edges = load_edges(EDGES_IN_CSV)

    EDGES_OUT_CSV.parent.mkdir(exist_ok=True)
    with EDGES_OUT_CSV.open("w", newline="", encoding="utf-8") as f_out:
        fieldnames = [
            ":START_ID(Hut)",
            ":END_ID(Hut)",
            "distance_km:float",
            "dplus_m:float",
            "dminus_m:float",
        ]
        writer = csv.DictWriter(f_out, fieldnames=fieldnames)
        writer.writeheader()

        for idx, (a_id, b_id) in enumerate(edges, start=1):
            hut_a = huts.get(a_id)
            hut_b = huts.get(b_id)
            if hut_a is None or hut_b is None:
                print(f"[{idx}/{len(edges)}] Hut manquante pour edge {a_id} -> {b_id}, on saute.")
                continue

            print(f"[{idx}/{len(edges)}] {hut_a['name']} -> {hut_b['name']}")

            result = call_ors(hut_a, hut_b)
            if result is None:
                distance_km = None
                dplus_m = None
                dminus_m = None
            else:
                distance_km, dplus_m, dminus_m = result

            writer.writerow({
                ":START_ID(Hut)": a_id,
                ":END_ID(Hut)": b_id,
                "distance_km:float": distance_km,
                "dplus_m:float": dplus_m,
                "dminus_m:float": dminus_m,
            })

            time.sleep(SLEEP_BETWEEN_CALLS)

    print(f"Fichier enrichi écrit dans {EDGES_OUT_CSV}")


if __name__ == "__main__":
    main()
