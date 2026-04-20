# Marketplace Ops Intelligence

Singapore taxi **supply depletion and imbalance intelligence** system.

This repo does not directly observe rider demand. Instead, it combines:
- real-time taxi availability and depletion signals
- public exogenous pressure proxies such as rainfall and congestion
- zone-level imbalance scoring
- heuristic intervention policy outputs for ops teams

The current ML model is a **near-term supply depletion risk classifier**. It is one layer of the system, not the entire business problem.

## Problem Statement

The operational question is not simply "where will taxis run out?" and it is not "what is true rider demand?".

This project currently aims to:
1. estimate **near-term supply depletion risk**
2. estimate **demand pressure** from public exogenous signals
3. combine those into a **zone-level imbalance signal**
4. support **intervention monitoring and future evaluation**

That framing is intentionally narrower and more honest than claiming direct demand estimation from taxi availability alone.

## System Architecture

```text
LTA / public signals
  → ingestion
  → preprocessing
  → depletion-risk model (LightGBM)
  → demand-pressure proxy scoring
  → imbalance scoring
  → baseline intervention policy + recommendation engine
  → FastAPI
  → React dashboard
```

### Layer 1: Supply Availability And Depletion Risk
- Taxi availability snapshots are ingested at zone level.
- Feature engineering builds lag, rolling, baseline, and depletion signals.
- A LightGBM classifier estimates the probability that available taxi count will drop materially in the next hour.

### Layer 2: Demand-Pressure Proxies
- Time-of-day and day-of-week patterns
- Weekend effects
- Rainfall intensity
- Traffic congestion
- Placeholder train disruption flag interface

These are **pressure proxies**, not direct demand labels.

### Layer 3: Imbalance Scoring
- Demand-pressure proxies are combined with live supply availability.
- The repo now computes bounded imbalance scores using:
  - ratio-style imbalance
  - normalized-difference imbalance

### Layer 4: Baseline Intervention Policy
- A heuristic policy maps depletion risk and imbalance into:
  - `no_action`
  - `watchlist`
  - `intervention`

This is a transparent baseline policy. It is not an optimized decision engine.

### Layer 5: Monitoring And Evaluation
- Batch scoring metadata
- drift monitoring
- registry / promotion / rollback
- recommendation outcome logging
- evaluation scaffolding for future treatment-vs-holdout comparisons

## What Exists Today

- ML pipeline for training and scoring
- model registry and promotion gate
- drift monitoring and operational health endpoints
- real-time taxi availability ingestion
- rainfall and congestion feature support
- recommendation engine and ops dashboard
- outcome logging for future policy learning

## Data Sources

Current and planned public-signal inputs:

- **Taxi availability**: LTA DataMall taxi snapshots
- **Weather**: Open-Meteo / rainfall features
- **Traffic congestion**: LTA travel-time derived congestion ratio
- **Calendar effects**: hour-of-day, weekday, weekend, holidays
- **Train disruption flag**: placeholder interface exists; live connector still TODO

## Models And Scores

### Depletion Risk Model
- Type: `LightGBMClassifier`
- Current target: large next-hour drop in available taxi count
- Existing compatibility field: `delay_risk_score`
- Preferred internal meaning: **depletion risk score**

### Demand Pressure Score
- Transparent heuristic composite from exogenous conditions
- Bounded in `[0, 1]`
- Does **not** claim to measure true demand directly

### Imbalance Score
- Derived from demand-pressure score and supply availability
- Intended as a descriptive decision-support signal
- Not a causal or equilibrium estimate

## Outputs

Core batch outputs in `data/outputs/`:
- `predictions.csv`
- `flagged_zones.csv`
- `recommended_actions.csv`
- `score_distribution.json`
- `zone_scores_history.jsonl`
- `pipeline.log`

Predictions now include additive descriptive fields such as:
- `depletion_risk_score`
- `demand_pressure_score`
- `imbalance_score`
- `imbalance_level`
- `policy_action`

Compatibility note:
- `delay_risk_score` remains in the API and flat files because the frontend still depends on it.

## Dashboard

The React dashboard surfaces:
- current depletion risk
- supply-state and rapid-depletion monitoring
- imbalance-aware recommendation context
- model health and drift
- reporting on recommendation outcomes

## Running Locally

### Install

```bash
make install
make install-frontend
```

### Train

```bash
make train
```

### Score

```bash
make score
```

### Start API

```bash
make api
```

### Start frontend

```bash
make frontend
```

## Live / Continuous Operation

To keep outputs fresh in local demo or operational runs:

```bash
make poller
make monitor-loop
```

`Retrain Model` creates a new model version. It does **not** refresh stale prediction outputs by itself.

## Limitations

- Rider demand is **not directly observed**.
- Demand pressure is approximated from public exogenous signals.
- The intervention layer is still heuristic and partly rule-driven.
- Outcome tracking exists, but causal evaluation is still scaffolding, not a finished experimentation system.
- Some API fields still retain legacy naming for compatibility.

## Future Work

- live train disruption connector
- richer event and crowding signals
- explicit treatment / holdout assignment in production policy flow
- offline intervention evaluation and uplift-style policy comparison
- optimized decision policy instead of heuristic baseline mapping

## API

Base URL:

```text
http://localhost:8000/api/v1
```

The API remains compatibility-first. Existing clients continue to work while newer depletion / imbalance fields are added incrementally.

Full schema notes: [API_CONTRACT.md](API_CONTRACT.md)
