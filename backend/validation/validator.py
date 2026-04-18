"""
Input Validation Pipeline
=========================
Responsibility: Gate raw data before it enters preprocessing. Isolates bad
rows rather than failing the entire batch (row-level fault tolerance).

Input schema (taxi availability-based supply signal):
    zone_id       : int      positive integer, 1–55
    timestamp     : datetime timezone-aware
    taxi_count    : int      available taxis in zone [0, 5000]
    weather_code  : int      WMO weather code
    is_holiday    : bool

Optional columns validated if present:
    rainfall_mm   : float    [0, 300]
    zone_type     : str      CBD / transport_hub / residential / mixed / industrial
    region        : str      Central / East / West / North / North-East

Outputs:
    clean_df    : pd.DataFrame  – rows that passed all checks
    failed_rows : pd.DataFrame  – rows that failed, with a 'failure_reason' column
"""

import pandas as pd

REQUIRED_COLUMNS = [
    "zone_id", "timestamp", "taxi_count", "weather_code", "is_holiday",
]

RANGE_RULES: dict[str, tuple[float, float]] = {
    "taxi_count":  (0, 5_000),
    "rainfall_mm": (0, 300),
}

VALID_WMO_CODES = {
    0, 1, 2, 3,
    45, 48,
    51, 53, 55,
    61, 63, 65,
    67, 71, 73, 75, 77,
    80, 81, 82,
    85, 86,
    95, 96, 99,
}

VALID_REGIONS    = {"Central", "East", "West", "North", "North-East"}
VALID_ZONE_TYPES = {"CBD", "transport_hub", "residential", "mixed", "industrial"}


def validate(df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Return (clean_df, failed_df).

    Never raises – always degrades gracefully. Bad rows are tagged with a
    'failure_reason' column and returned separately so they can be written
    to data/outputs/failed_rows.csv for audit.
    """
    if df.empty:
        empty_fail = df.copy()
        empty_fail["failure_reason"] = pd.Series(dtype="string")
        return df.copy(), empty_fail

    df = df.copy()
    failure_reasons: dict[int, list[str]] = {i: [] for i in df.index}

    # 1. Schema check – required columns present
    missing_cols = [c for c in REQUIRED_COLUMNS if c not in df.columns]
    if missing_cols:
        fail_df = df.copy()
        fail_df["failure_reason"] = f"missing columns: {missing_cols}"
        return pd.DataFrame(columns=df.columns), fail_df

    # 2. Missing value check on required fields
    for col in REQUIRED_COLUMNS:
        null_mask = df[col].isna()
        for idx in df.index[null_mask]:
            failure_reasons[idx].append(f"null_{col}")

    # 3. Range checks
    for col, (lo, hi) in RANGE_RULES.items():
        if col not in df.columns:
            continue
        out_of_range = df[col].notna() & ((df[col] < lo) | (df[col] > hi))
        for idx in df.index[out_of_range]:
            failure_reasons[idx].append(f"{col}_out_of_range({df.at[idx, col]:.2f})")

    # 4. Weather code category check
    if "weather_code" in df.columns:
        bad_wmo = df["weather_code"].notna() & ~df["weather_code"].isin(VALID_WMO_CODES)
        for idx in df.index[bad_wmo]:
            failure_reasons[idx].append(f"invalid_weather_code({df.at[idx, 'weather_code']})")

    # 5. Zone id must be a positive integer
    if "zone_id" in df.columns:
        bad_zone = df["zone_id"].notna() & (df["zone_id"] < 1)
        for idx in df.index[bad_zone]:
            failure_reasons[idx].append(f"invalid_zone_id({df.at[idx, 'zone_id']})")

    # 6. Region category check (optional column)
    if "region" in df.columns:
        bad_region = df["region"].notna() & ~df["region"].isin(VALID_REGIONS)
        for idx in df.index[bad_region]:
            failure_reasons[idx].append(f"invalid_region({df.at[idx, 'region']})")

    # 7. Zone type category check (optional column)
    if "zone_type" in df.columns:
        bad_type = df["zone_type"].notna() & ~df["zone_type"].isin(VALID_ZONE_TYPES)
        for idx in df.index[bad_type]:
            failure_reasons[idx].append(f"invalid_zone_type({df.at[idx, 'zone_type']})")

    # Split clean vs failed
    failed_indices = [i for i, reasons in failure_reasons.items() if reasons]
    clean_indices  = [i for i, reasons in failure_reasons.items() if not reasons]

    clean_df = df.loc[clean_indices].copy()

    if failed_indices:
        fail_df = df.loc[failed_indices].copy()
        fail_df["failure_reason"] = [
            "; ".join(failure_reasons[i]) for i in failed_indices
        ]
    else:
        fail_df = df.iloc[0:0].copy()
        fail_df["failure_reason"] = pd.Series(dtype="string")

    return clean_df.reset_index(drop=True), fail_df.reset_index(drop=True)
