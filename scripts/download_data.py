"""
Data Download Script — Singapore Context
=========================================
Downloads raw data from Singapore government open data sources and Kaggle.

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
DATASET 1 — LTA DataMall: Taxi Availability (Primary)
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Source  : https://datamall.lta.gov.sg/
API key : Free — register at https://datamall.lta.gov.sg/content/datamall/en/request-for-api.html
Endpoint: GET https://datamall2.mytransport.sg/ltaodataservice/Taxi-Availability
Format  : JSON — list of {Latitude, Longitude} for every available taxi in SG
Strategy: Poll every 5 minutes, save hourly snapshots, aggregate to zone-hour.

What you get per snapshot:
    - Number of available taxis per planning area (supply_proxy)
    - Spatial distribution of supply across SG

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
DATASET 2 — LTA DataMall: Passenger Volume by OD (Demand Proxy)
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Source  : https://datamall.lta.gov.sg/content/datamall/en/dynamic-data.html
Format  : Monthly CSV download (requires LTA API key)
Endpoint: POST https://datamall2.mytransport.sg/ltaodataservice/PV/ODTrain
Columns : YEAR_MONTH, DAY_TYPE, TIME_PER_HOUR, PT_TYPE, ORIGIN_PT_CODE,
          DESTINATION_PT_CODE, TOTAL_TRIPS
Strategy: Use ORIGIN_PT_CODE → planning area join to get demand per zone per hour.

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
DATASET 3 — Grab-Posisi (Kaggle, GPS traces)
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Kaggle   : https://www.kaggle.com/datasets/oscarwyd/grab-posisi
Requires : kaggle CLI  — pip install kaggle
           API key in ~/.kaggle/kaggle.json
Columns  : trj_id, pingtimestamp, rawlat, rawlng, speed, bearing, accuracy
Filter   : Singapore bbox — lat [1.15, 1.48], lon [103.6, 104.1]
Strategy : Driver ping density per planning area per hour → supply_proxy

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
DATASET 4 — data.gov.sg: Singapore Public Holidays
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Source  : https://data.gov.sg/datasets/d_3751791452397f1b1c80a8db5fc7d83c/view
Format  : JSON via CKAN API — no key required
Use     : is_holiday feature in preprocessing pipeline

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
DATASET 5 — URA Planning Area GeoJSON (Zone Lookup)
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Source   : https://data.gov.sg/collections/2104/view
Format   : GeoJSON — 55 planning area polygons with names
Used for : Spatial join (lat/lon → planning area), zone_lookup table

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

Usage:
    python scripts/download_data.py --source lta       # Taxi availability snapshots
    python scripts/download_data.py --source grab      # Grab-Posisi (requires kaggle key)
    python scripts/download_data.py --source holidays  # SG public holidays
    python scripts/download_data.py --source zones     # URA planning area GeoJSON
    python scripts/download_data.py --all              # Everything
"""

import argparse
import json
import os
import requests
from pathlib import Path

RAW_DIR = Path("data/raw")
RAW_DIR.mkdir(parents=True, exist_ok=True)

LTA_API_KEY = os.getenv("LTA_API_KEY", "")  # set in .env


def download_taxi_availability_snapshot() -> dict:
    """
    Poll LTA DataMall Taxi-Availability endpoint once.
    Returns raw JSON response (list of {Latitude, Longitude}).
    Requires LTA_API_KEY env var.
    """
    url = "https://datamall2.mytransport.sg/ltaodataservice/Taxi-Availability"
    headers = {"AccountKey": LTA_API_KEY, "accept": "application/json"}
    resp = requests.get(url, headers=headers, timeout=10)
    resp.raise_for_status()
    return resp.json()


def download_passenger_volume(year_month: str) -> None:
    """
    Request LTA passenger volume by OD for a given month (e.g. "202401").
    Saves CSV to data/raw/passenger_volume_{year_month}.csv
    Note: LTA sends a download link via email after request — manual step.
    """
    raise NotImplementedError


def download_grab_posisi() -> None:
    """
    Download Grab-Posisi dataset from Kaggle.
    Requires ~/.kaggle/kaggle.json with API credentials.
    Saves to data/raw/grab_posisi/
    """
    import subprocess
    out = RAW_DIR / "grab_posisi"
    out.mkdir(exist_ok=True)
    subprocess.run(
        ["kaggle", "datasets", "download", "-d", "oscarwyd/grab-posisi",
         "--unzip", "-p", str(out)],
        check=True,
    )
    print(f"Grab-Posisi saved to {out}")


def download_sg_holidays() -> None:
    """
    Download Singapore public holidays from data.gov.sg API.
    Saves to data/raw/sg_public_holidays.json
    """
    url = ("https://data.gov.sg/api/action/datastore_search"
           "?resource_id=d_3751791452397f1b1c80a8db5fc7d83c&limit=100")
    resp = requests.get(url, timeout=10)
    resp.raise_for_status()
    out = RAW_DIR / "sg_public_holidays.json"
    out.write_text(json.dumps(resp.json(), indent=2))
    print(f"Holidays saved to {out}")


def download_ura_planning_areas() -> None:
    """
    Download URA planning area GeoJSON from data.gov.sg.
    Used for spatial joins (lat/lon → planning area name).
    Saves to data/raw/sg_planning_areas.geojson
    """
    url = "https://geo.data.gov.sg/planning-area-census2010/2014-10-07/geojson/planning-area-census2010.geojson"
    resp = requests.get(url, timeout=30)
    resp.raise_for_status()
    out = RAW_DIR / "sg_planning_areas.geojson"
    out.write_bytes(resp.content)
    print(f"Planning areas GeoJSON saved to {out}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Download SG marketplace data")
    parser.add_argument("--source", choices=["lta", "grab", "holidays", "zones"])
    parser.add_argument("--all", action="store_true", help="Download all sources")
    args = parser.parse_args()

    if args.all or args.source == "lta":
        data = download_taxi_availability_snapshot()
        out = RAW_DIR / "taxi_availability_sample.json"
        out.write_text(json.dumps(data, indent=2))
        print(f"Taxi availability snapshot saved to {out}")

    if args.all or args.source == "grab":
        download_grab_posisi()

    if args.all or args.source == "holidays":
        download_sg_holidays()

    if args.all or args.source == "zones":
        download_ura_planning_areas()
