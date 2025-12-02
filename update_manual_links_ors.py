import csv
import os
import time
import json
from pathlib import Path

import requests

BASE_DIR = Path(__file__).resolve().parent
HUTS_CSV = BASE_DIR / "neo4j_huts" / "huts.csv"

ORS_API_KEY = os.environ.get("ORS_API_KEY")
if not ORS_API_KEY:
    raise RuntimeError(
        "Variable d'environnement ORS_API_KEY non définie. "
        "Définis-la avant de lancer ce script."
    )

ORS_URL = "https://api.openrouteservice.org/v2/directions/foot-hiking"
SLEEP_BETWEEN_CALLS = 2.0  # secondes entre les appels pour rester gentil avec l'API

# --------------------------------------------------------------------
# À ADAPTER SI BESOIN : liste des liens créés manuellement dans Neo4j
# --------------------------------------------------------------------
# Les noms doivent correspondre EXACTEMENT au champ 'name' de huts.csv
MANUAL_EDGES = [
    ("Vistasstuga", "Fjällstuga Nallo"),
    ("Vistasstuga", "Alesjaure Fjällstuga"),
    ("Fjällstuga Nallo", "Sälka Fjällstuga"),
    ("STF Abisko Turiststation", "Kårsavagge Fjällstuga"),
    ("STF Abisko Turiststation", "Låktatjåkkastugan"),
    ("Vaisaluokta Fjällstuga", "Akka Fjällstuga"),
    ("Vaisaluokta Fjällstuga", "Kutjaure Fjällstuga"),
    ("Tarfala", "Kebnekaise Restaurant"),
    ("Tarfala", "Singi Fjällstuga"),
]


def load_huts_by_name(path: Path):
    huts = {}
    with path.open(newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            name = row["name"]
            if name in huts:
                print(f"ATTENTION: nom dupliqué dans huts.csv : {name}")
            huts[name] = {
                "hut_id": int(row["hut_id:ID(Hut)"]),
                "lat": float(row["latitude:float"]),
                "lon": float(row["longitude:float"]),
                "name": name,
            }
    print(f"{len(huts)} huts chargées depuis {path}")
    return huts


def call_ors(hut_a, hut_b):
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

    # Cas GeoJSON (features)
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

    # Cas JSON routes
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

    distance_km = distance_m / 1000.0
    return distance_km, ascent, descent


def main():
    huts = load_huts_by_name(HUTS_CSV)

    print("\n-- Requêtes Cypher à exécuter dans Neo4j pour mettre à jour les liens manuels --\n")

    for (name_a, name_b) in MANUAL_EDGES:
        hut_a = huts.get(name_a)
        hut_b = huts.get(name_b)

        if hut_a is None or hut_b is None:
            print(f"-- SKIP: Hut introuvable dans huts.csv pour le couple ({name_a}, {name_b})")
            continue

        print(f"Calcul ORS pour {name_a} -> {name_b} ...")
        result = call_ors(hut_a, hut_b)
        if result is None:
            print(f"-- ERREUR ORS pour {name_a} -> {name_b}, lien non mis à jour.\n")
            continue

        distance_km, dplus, dminus = result

        # On arrondit un peu pour éviter les nombres à rallonge
        distance_km_r = round(distance_km, 3)
        dplus_r = round(dplus, 1)
        dminus_r = round(dminus, 1)

        print(f"""
// {name_a} <-> {name_b}
MATCH (a:Hut {{name:"{name_a}"}}), (b:Hut {{name:"{name_b}"}})
MERGE (a)-[l1:LINK]->(b)
SET l1.distance_km = {distance_km_r},
    l1.dplus_m     = {dplus_r},
    l1.dminus_m    = {dminus_r};

MERGE (b)-[l2:LINK]->(a)
SET l2.distance_km = {distance_km_r},
    l2.dplus_m     = {dminus_r},
    l2.dminus_m    = {dplus_r};
""")

        time.sleep(SLEEP_BETWEEN_CALLS)


if __name__ == "__main__":
    main()
