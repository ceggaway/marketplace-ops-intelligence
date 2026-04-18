"""
Singapore Weather Client
========================
Fetches hourly weather data for Singapore from two sources:

Primary — Open-Meteo (free, no API key required):
    URL: https://api.open-meteo.com/v1/forecast
    Covers Singapore centroid (lat=1.3521, lon=103.8198).
    Returns: temperature_2m, precipitation, weathercode, windspeed_10m
    Docs: https://open-meteo.com/en/docs

Secondary — NEA (National Environment Agency) Real-Time API:
    Station rainfall: https://api.data.gov.sg/v1/environment/rainfall
    Temperature:      https://api.data.gov.sg/v1/environment/air-temperature
    Relative humidity: https://api.data.gov.sg/v1/environment/relative-humidity

    NEA has ~65 weather stations across SG — useful for zone-level granularity.
    Key stations by region:
        Central : S24 (Clementi), S43 (Kim Chuan Road)
        East    : S107 (East Coast Pkwy), S108 (Changi)
        North   : S70 (Admiralty), S71 (Kranji)
        West    : S44 (Nanyang Ave), S117 (Banyan Road)
        North-E : S113 (Serangoon North)

    Data source: National Environment Agency (NEA) via data.gov.sg
    API: https://api.data.gov.sg/v1/environment/
    Licence: Singapore Open Data Licence v1.0 (https://data.gov.sg/open-data-licence)

Singapore-specific weather patterns:
    - Northeast Monsoon: Nov–Jan (heavy rain, flooding risk)
    - Inter-monsoon   : Apr–May, Oct (afternoon thunderstorms, demand spikes)
    - Southwest Monsoon: Jun–Sep (drier, still afternoon storms)
    - Peak rain hours : 14:00–18:00 SGT (afternoon convective storms)

WMO weather codes relevant to SG operations:
    61–67  : Rain (various intensities)
    80–82  : Rain showers (common in SG afternoons)
    95–99  : Thunderstorms (highest demand impact)
    is_raining = weather_code in {61,63,65,67,80,81,82,95,96,99}
"""

import pandas as pd

SG_LAT = 1.3521
SG_LON = 103.8198

RAIN_CODES = {61, 63, 65, 67, 80, 81, 82, 95, 96, 99}

# NEA station → URA planning area mapping (nearest weather station per zone).
# Station locations sourced from data.gov.sg NEA station metadata.
# Stations cover all 5 regions; zones without a closer station fall back to
# the nearest regional station.
#
# Key stations used:
#   S24  – Clementi Road          (West/Central)
#   S43  – Kim Chuan Road         (Central/East)
#   S44  – Nanyang Avenue         (West)
#   S50  – Clementi               (West)
#   S60  – Sentosa Island         (South/Central)
#   S66  – Tuas South             (West industrial)
#   S70  – Admiralty               (North)
#   S71  – Kranji Reservoir        (North)
#   S90  – Pulau Ubin              (North-East islands)
#   S100 – Woodlands Ave 9         (North)
#   S106 – Tai Seng               (Central/East)
#   S107 – East Coast Pkwy         (East)
#   S108 – Changi                  (East)
#   S109 – Ang Mo Kio Ave 5        (North-East)
#   S111 – Scotts Road             (Central)
#   S115 – Toa Payoh               (Central)
#   S116 – West Coast Highway      (West)
#   S117 – Banyan Road             (West)
#   S121 – Old Choa Chu Kang Rd    (West/North)
#   S122 – Semakau                 (South)
#   S200 – Pasir Panjang           (West/Central)
NEA_ZONE_MAP: dict[str, str] = {
    # Central
    "Downtown Core":    "S111",   # Scotts Rd — nearest to CBD core
    "Marina East":      "S111",
    "Marina South":     "S60",    # Sentosa / south waterfront
    "Museum":           "S111",
    "Newton":           "S111",
    "Novena":           "S115",   # Toa Payoh
    "Orchard":          "S111",
    "Outram":           "S24",    # Clementi Rd (nearest central-south)
    "River Valley":     "S111",
    "Rochor":           "S43",    # Kim Chuan (central-east)
    "Singapore River":  "S111",
    "Straits View":     "S60",
    "Bukit Merah":      "S200",   # Pasir Panjang
    "Bukit Timah":      "S24",
    "Queenstown":       "S200",
    "Tanglin":          "S111",
    "Toa Payoh":        "S115",
    "Bishan":           "S115",
    "Kallang":          "S43",
    "Geylang":          "S43",
    "Marine Parade":    "S107",   # East Coast Pkwy
    "Paya Lebar":       "S43",
    "Bukit Brown":      "S115",
    "Serangoon":        "S109",   # Ang Mo Kio Ave 5
    # East
    "Bedok":            "S107",
    "Changi":           "S108",
    "Pasir Ris":        "S108",
    "Tampines":         "S107",
    # North-East
    "Ang Mo Kio":       "S109",
    "Hougang":          "S109",
    "Punggol":          "S109",
    "Seletar":          "S109",
    "Sengkang":         "S109",
    # North
    "Lim Chu Kang":     "S71",    # Kranji Reservoir
    "Mandai":           "S71",
    "Sembawang":        "S70",    # Admiralty
    "Simpang":          "S71",
    "Sungei Kadut":     "S100",   # Woodlands Ave 9
    "Woodlands":        "S100",
    "Yishun":           "S70",
    # West
    "Boon Lay":         "S117",   # Banyan Road
    "Bukit Batok":      "S121",   # Old Choa Chu Kang Rd
    "Bukit Panjang":    "S121",
    "Buona Vista":      "S116",   # West Coast Hwy
    "Choa Chu Kang":    "S121",
    "Clementi":         "S50",    # Clementi station
    "Jurong East":      "S44",    # Nanyang Ave
    "Jurong West":      "S44",
    "Pioneer":          "S66",    # Tuas South
    "Tengah":           "S121",
    "Tengah Park":      "S121",
    "Tuas":             "S66",
    "Western Islands":  "S60",    # Sentosa as proxy (offshore)
    "Western Water":    "S66",
    # Central outliers
    "Southern Islands": "S60",
    "Harbourfront":     "S60",
}

# Open-Meteo API endpoint
_OPEN_METEO_URL = "https://api.open-meteo.com/v1/forecast"


def get_hourly_weather(start: str, end: str) -> pd.DataFrame:
    """
    Fetch hourly weather from Open-Meteo for Singapore.
    Free — no API key required.

    Args:
        start: ISO date string, e.g. "2024-01-01"
        end  : ISO date string, e.g. "2024-01-31"

    Returns DataFrame with columns:
        timestamp, temperature_c, rainfall_mm, windspeed_kmh,
        weather_code, is_raining

    Data source: Open-Meteo (https://open-meteo.com)
    Licence: CC BY 4.0
    """
    try:
        import requests
    except ImportError:
        raise RuntimeError("requests not installed — run: pip install requests")

    params = {
        "latitude":   SG_LAT,
        "longitude":  SG_LON,
        "hourly":     "temperature_2m,precipitation,weathercode,windspeed_10m",
        "timezone":   "Asia/Singapore",
        "start_date": start,
        "end_date":   end,
    }
    resp = requests.get(_OPEN_METEO_URL, params=params, timeout=30)
    resp.raise_for_status()
    hourly = resp.json()["hourly"]

    df = pd.DataFrame({
        "timestamp":     pd.to_datetime(hourly["time"]),
        "temperature_c": hourly["temperature_2m"],
        "rainfall_mm":   hourly["precipitation"],
        "weather_code":  hourly["weathercode"],
        "windspeed_kmh": hourly["windspeed_10m"],
    })
    df["timestamp"] = df["timestamp"].dt.tz_localize("Asia/Singapore")
    df["is_raining"] = df["weather_code"].isin(RAIN_CODES)
    return df


def get_nea_rainfall(station_id: str, timestamp: str) -> float:
    """
    Fetch 5-minute rainfall reading (mm) from a specific NEA station.

    Args:
        station_id: NEA station code, e.g. "S24"
        timestamp:  ISO datetime string, e.g. "2024-01-01T08:00:00"

    Returns: rainfall in mm (float)

    Data source: National Environment Agency (NEA) via data.gov.sg
    API: https://api.data.gov.sg/v1/environment/rainfall
    Licence: Singapore Open Data Licence v1.0
    """
    try:
        import requests
    except ImportError:
        raise RuntimeError("requests not installed — run: pip install requests")

    resp = requests.get(
        "https://api.data.gov.sg/v1/environment/rainfall",
        params={"date_time": timestamp},
        timeout=15,
    )
    resp.raise_for_status()
    readings = resp.json().get("items", [{}])[0].get("readings", [])
    for r in readings:
        if r.get("station_id") == station_id:
            return float(r.get("value", 0.0))
    return 0.0


def get_zone_rainfall(zone_lookup: pd.DataFrame, timestamp: str) -> pd.DataFrame:
    """
    For each zone in zone_lookup, return nearest NEA station rainfall reading.

    Returns DataFrame: zone_id, zone_name, rainfall_mm, is_raining

    Data source: National Environment Agency (NEA) via data.gov.sg
    Licence: Singapore Open Data Licence v1.0
    """
    try:
        import requests
    except ImportError:
        raise RuntimeError("requests not installed — run: pip install requests")

    resp = requests.get(
        "https://api.data.gov.sg/v1/environment/rainfall",
        params={"date_time": timestamp},
        timeout=15,
    )
    resp.raise_for_status()
    readings = {
        r["station_id"]: float(r.get("value", 0.0))
        for r in resp.json().get("items", [{}])[0].get("readings", [])
    }

    rows = []
    for _, zone in zone_lookup.iterrows():
        station = NEA_ZONE_MAP.get(zone["zone_name"], "S24")
        mm = readings.get(station, 0.0)
        rows.append({
            "zone_id":    int(zone["zone_id"]),
            "zone_name":  zone["zone_name"],
            "rainfall_mm": mm,
            "is_raining":  mm > 0.1,
        })
    return pd.DataFrame(rows)
