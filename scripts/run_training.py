"""
Training Pipeline Runner
========================
Full pipeline: generate → validate → preprocess → train → register → gate → promote

Usage:
    python scripts/run_training.py --version v1
    python scripts/run_training.py --version v2 --days 90
    python scripts/run_training.py --version v1 --skip-gate
"""

import argparse
import subprocess
import sys
import time
from pathlib import Path

# Ensure project root is on sys.path when run as a script
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

try:
    from dotenv import load_dotenv
    load_dotenv(Path(__file__).resolve().parents[1] / ".env")
except ImportError:
    pass

import pandas as pd

from backend.ingestion.loader import generate_synthetic_data
from backend.validation.validator import validate
from backend.preprocessing.pipeline import build_features
from backend.training.trainer import train
from backend.registry import model_registry as registry
from backend.promotion.gate import run_gate

TRAINING_PARQUET = Path("data/processed/training.parquet")
SNAPSHOT_DIR     = Path("data/raw/taxi_snapshots")


def main():
    parser = argparse.ArgumentParser(description="Run the training pipeline")
    parser.add_argument("--version",        default="v1",  help="Version id (e.g. v1, v2)")
    parser.add_argument("--days",           type=int, default=90, help="Days of synthetic data (only with --allow-synthetic)")
    parser.add_argument("--skip-gate",      action="store_true",  help="Skip promotion gate and auto-promote")
    parser.add_argument("--allow-synthetic", action="store_true",
                        help="Allow training on synthetic data when no real snapshots exist. "
                             "NEVER use for production model promotion.")
    parser.add_argument("--data",           default=None,
                        help="Path to a pre-built training parquet (overrides auto-detect)")
    args = parser.parse_args()

    print(f"\n{'='*60}")
    print(f"  Training Pipeline — version {args.version}")
    print(f"{'='*60}\n")

    # ── Step 1: Load real data or fall back to synthetic ────────────────────
    t0          = time.time()
    using_real  = False

    # Explicit path takes precedence
    if args.data:
        data_path = Path(args.data)
        if not data_path.exists():
            print(f"ERROR: --data path not found: {data_path}")
            sys.exit(1)
        print(f"[1/6] Loading training dataset from {data_path}...")
        feature_df = pd.read_parquet(data_path)
        using_real = True
        shortage_rt = (feature_df["supply_shortage"] == 1).mean() if "supply_shortage" in feature_df.columns else float("nan")
        print(f"      {len(feature_df):,} rows  |  shortage rate: {shortage_rt:.2%}  [{time.time()-t0:.1f}s]")

    elif TRAINING_PARQUET.exists():
        # Pre-built dataset from `python scripts/build_training_data.py`
        print("[1/6] Loading pre-built real training dataset...")
        feature_df  = pd.read_parquet(TRAINING_PARQUET)
        shortage_rt = (feature_df["supply_shortage"] == 1).mean() if "supply_shortage" in feature_df.columns else float("nan")
        print(f"      {len(feature_df):,} rows  |  shortage rate: {shortage_rt:.2%}  "
              f"(source: {TRAINING_PARQUET})  [{time.time()-t0:.1f}s]")
        # Fall back to synthetic if real data has no positive examples to learn from
        if "supply_shortage" in feature_df.columns and feature_df["supply_shortage"].sum() == 0:
            if args.allow_synthetic:
                print(f"      WARNING: real data has 0 positive examples — falling back to {args.days}d synthetic.")
                raw_df = generate_synthetic_data(days=args.days)
                feature_df = None  # steps 2+3 will build it from raw_df
                print(f"      {len(raw_df):,} synthetic rows generated  [{time.time()-t0:.1f}s]")
            else:
                print("      WARNING: real data has 0 positive examples. Pass --allow-synthetic to fall back.")
                using_real = True  # continue with broken data — gate will reject it
        if feature_df is not None:
            using_real = True

    else:
        n_snapshots = len(list(SNAPSHOT_DIR.glob("*.csv")))
        if n_snapshots >= 12:
            # At least 1 hour of 5-min snapshots — build the parquet now
            print(f"[1/6] Building training data from {n_snapshots} real snapshots "
                  f"(~{n_snapshots/12:.0f}h)...")
            result = subprocess.run(
                [sys.executable, str(Path(__file__).parent / "build_training_data.py"),
                 "--out", str(TRAINING_PARQUET), "--min-hours", "1"],
                capture_output=True, text=True,
            )
            if result.returncode == 0 and TRAINING_PARQUET.exists():
                feature_df = pd.read_parquet(TRAINING_PARQUET)
                using_real = True
                print(f"      {len(feature_df):,} rows built from real snapshots  [{time.time()-t0:.1f}s]")
            else:
                print(f"      Build failed.")
                if result.stderr:
                    print(f"      {result.stderr.strip()}")
        else:
            print(f"[1/6] No real data found ({n_snapshots} snapshots, need ≥12).")
            print(f"      Start the LTA poller to collect real data:")
            print(f"        python -m backend.ingestion.lta_poller")

        if not using_real:
            if not args.allow_synthetic:
                print("\n" + "="*60)
                print("  ERROR: No real training data available.")
                print("  To train on synthetic data for development/testing, pass:")
                print("    --allow-synthetic")
                print("  Do NOT use synthetic models in production.")
                print("="*60 + "\n")
                sys.exit(1)
            print(f"      [--allow-synthetic] Generating {args.days} days of synthetic data...")
            raw_df = generate_synthetic_data(days=args.days)
            print(f"      {len(raw_df):,} rows generated in {time.time()-t0:.1f}s")

    # ── Step 2: Validate (skipped for pre-built real data) ───────────────────
    if not using_real:
        print("[2/6] Validating raw data...")
        clean_df, failed_df = validate(raw_df)
        pct_failed = len(failed_df) / len(raw_df) * 100 if len(raw_df) > 0 else 0
        print(f"      Clean: {len(clean_df):,}  |  Failed: {len(failed_df):,} ({pct_failed:.1f}%)")
        if not failed_df.empty:
            Path("data/outputs").mkdir(parents=True, exist_ok=True)
            failed_df.to_csv("data/outputs/failed_rows.csv", index=False)
        if clean_df.empty:
            print("ERROR: No clean data after validation. Aborting.")
            sys.exit(1)
    else:
        print("[2/6] Validation skipped — dataset is pre-validated.")
        clean_df = feature_df  # already clean

    # ── Step 3: Preprocess / feature engineering (skipped for pre-built data) ─
    if not using_real:
        print("[3/6] Building features...")
        t0 = time.time()
        feature_df = build_features(clean_df)
        print(f"      {len(feature_df):,} rows × {len(feature_df.columns)} features in {time.time()-t0:.1f}s")
        Path("data/processed").mkdir(parents=True, exist_ok=True)
        feature_df.to_parquet("data/processed/features.parquet", index=False)
        print("      Saved → data/processed/features.parquet")
    else:
        print(f"[3/6] Feature engineering skipped — using pre-built features "
              f"({len(feature_df.columns)} columns).")

    # ── Step 4: Train ────────────────────────────────────────────────────────
    print(f"[4/6] Training LightGBM model (version {args.version})...")
    t0 = time.time()
    metrics = train(feature_df, version_id=args.version)
    print(f"      Done in {time.time()-t0:.1f}s")
    print(f"      F1={metrics['f1']:.4f}  AUC={metrics['roc_auc']:.4f}  "
          f"Precision={metrics['precision']:.4f}  Recall={metrics['recall']:.4f}")

    # ── Step 5: Register ─────────────────────────────────────────────────────
    print(f"[5/6] Registering version {args.version} in model registry...")
    registry.register(args.version)
    print("      Registered as candidate.")

    # ── Step 6: Promotion gate ────────────────────────────────────────────────
    if args.skip_gate:
        print("[6/6] Promotion gate SKIPPED (--skip-gate). Auto-promoting...")
        registry.promote(args.version)
        print(f"      Version {args.version} is now ACTIVE.")
    else:
        print(f"[6/6] Running promotion gate...")
        result = run_gate(args.version)
        print(f"      Checks: {result.checks}")
        if result.passed:
            registry.promote(args.version)
            print(f"      PASSED — version {args.version} is now ACTIVE.")
        else:
            print(f"      FAILED — {result.reason}")
            print(f"      Version {args.version} remains as candidate.")

    print(f"\n{'='*60}")
    print("  Training pipeline complete.")
    print(f"{'='*60}\n")


if __name__ == "__main__":
    main()
