# Marketplace Ops Intelligence PRD

Version: 0.4  
Owner: Cedric  
Status: Current implementation spec  
Last updated: 2026-05-02

## 1. Product Summary

Marketplace Ops Intelligence is a supply-side operations dashboard for Singapore taxi operators. It monitors zone-level taxi availability, estimates near-term supply depletion risk, recommends human-approved intervention actions, and tracks whether recommendations and models are working.

The product does not estimate true rider demand, calculate prices, dispatch drivers, or auto-fire interventions. It uses public supply and stressor signals to support an ops team decision loop:

1. See what is happening.
2. Diagnose where and why supply is at risk.
3. Recommend a governed action.
4. Learn from model health and outcome signals.

## 2. Personas

Primary user: Ops analyst  
The analyst monitors live supply risk, identifies zones that need attention, reviews recommendations, and records whether recommended actions were followed.

Secondary user: Ops lead  
The lead reviews high-priority recommendations, approves higher-tier actions, monitors effectiveness, and uses reports for weekly operating reviews.

## 3. Scope

In scope:
- Project brief page describing product scope, architecture, pipeline flow, and deployment surfaces.
- Real-time overview of supply depletion risk across Singapore zones.
- Zone drilldown with risk scores, supply signals, key drivers, and recommendations.
- Optional H3 hex-cell drilldown when H3 predictions have been generated.
- Recommendation queue with priority, confidence, estimated impact, cost, governance tier, and follow-through logging.
- Model monitoring with AUC, precision, recall, PSI drift, run history, alerts, and retraining trigger.
- Outcome reports for zone performance, intervention follow-through, and model impact.

Out of scope:
- Rider demand estimation.
- Dispatch assignment.
- Fare or surge pricing.
- Driver-facing UI.
- Automatic intervention firing.
- Auth, SOC2 audit logging, and production operator-system integrations.

## 4. Core Data Concepts

Depletion risk score: Probability-like model output indicating near-term supply depletion risk. The legacy field name `delay_risk_score` remains for compatibility.

Risk level:
- High: score >= configured high threshold.
- Medium: score >= configured medium threshold and below high.
- Low: below medium threshold.

Predicted shortage: Bounded shortage/stress index combining depletion risk, exogenous demand-pressure proxies, and supply gap against baseline.

Demand pressure score: Heuristic exogenous-pressure proxy using calendar, rainfall, congestion, and related public signals. It is not true rider demand.

Imbalance score: Composite signal combining pressure and supply availability.

Action governance tier:
- Tier 0: Monitor only. No approver.
- Tier 1: Ops alert. Ops analyst.
- Tier 2: Driver-comms recommendation. Ops lead.
- Tier 3: Incentive proposal. Ops lead + Finance.
- Tier 4: Rebalancing recommendation. Ops lead, or Ops lead + Finance when combined with incentive.

Estimated recoverable opportunity: Illustrative opportunity estimate, not expected revenue. It combines depletion minutes, zone commercial weight, stressor intensity, historical supply gap proxy, and margin assumption.

## 5. Dashboard 0: Project Brief

Route: `/project`

Question answered: What does the system do, how is it wired, and what should a reviewer know before operating or deploying it?

The Project Brief page summarizes:
- product purpose and non-claims
- end-to-end ML pipeline
- system architecture
- daily batch workflow
- dashboard map
- optional H3 hex-cell spatial mode and operational caveats

Acceptance criteria:
- A reviewer can understand the product, architecture, and deployment model without opening the codebase.
- H3 instructions clearly state that H3 scoring requires prepared `h3_supply_features.csv`.

## 6. Dashboard 1: Overview

Route: `/`

Question answered: What is happening right now?

Primary endpoint dependencies:
- `GET /api/v1/overview`
- `GET /api/v1/zones`
- `GET /api/v1/pipeline/latest-run`
- `GET /api/v1/model/status`
- `GET /api/v1/health/services`

Refresh behavior:
- Overview payload refreshes every 60 seconds.
- Service health refreshes every 120 seconds.

### Cards And KPIs

High-Risk Zones:
- Shows count of zones currently classified as high risk out of 55.
- Shows medium-risk count as secondary context.
- Includes a sparkline of high-risk zone trend.
- Purpose: fastest indicator of whether ops attention is needed now.

Active Supply:
- Shows total active taxi supply across zones.
- Shows approximate taxis per zone.
- Includes a supply sparkline.
- Purpose: indicates citywide supply availability and whether depletion is broad or localized.

Rapid Depletion:
- Shows count of zones losing supply at greater than 30% per hour.
- Includes a depletion sparkline.
- Purpose: leading indicator before a zone becomes high risk.

Actions Needed:
- Shows count of critical and high-priority recommendations pending.
- Splits critical and high in supporting text.
- Purpose: bridges monitoring into action workflow.

Model Status:
- Shows latest PSI drift status and minutes since last scoring run.
- Shows production model version when available.
- Purpose: tells users whether current scores are operationally trustworthy.

### Main Panels

Top Risk Zones:
- Lists the highest-risk zones by depletion risk score.
- Shows rank, zone name, region, risk score, current supply, depletion rate, and risk badge.
- Clicking a row navigates to Zone Risk Monitor for drilldown.

Top Insights:
- Summarizes high-risk zones, rapid depletion, model drift, and pending actions.
- Uses status badges to distinguish immediate action, monitoring, stable, and action available.

High-Risk Zone Count Trend:
- Time series of high-risk zone count.
- User can filter to last 6 hours, last 24 hours, or last 7 days.
- Purpose: identifies whether risk is worsening or resolving.

System Status:
- Shows service state for Prediction API, Data Pipeline, Feature Store, Model Serving, and Drift Monitor.
- Uses backend service health when available; otherwise falls back to named services.

Recent Model Run:
- Shows run status, active taxis, flagged zones, failed rows, latency, and timestamp.
- Purpose: operational recency and scoring quality.

Recommended Next Action:
- Converts current overview state into a plain operational recommendation.
- Also lists focus zones from the current top-risk set.

Acceptance criteria:
- User can identify whether the system is healthy within 10 seconds.
- User can identify the worst zones and navigate to drilldown in one click.
- Stale API or missing data surfaces as an explicit unavailable/empty state.

## 7. Dashboard 2: Zone Risk Monitor

Route: `/zones`

Question answered: Which zones are risky, and why?

Primary endpoint dependencies:
- `GET /api/v1/zones`
- `GET /api/v1/zones/{zone_id}`
- `GET /api/v1/overview`
- `GET /api/v1/pipeline/latest-run`
- `GET /api/v1/monitoring/drift`
- optional H3 mode:
  - `GET /api/v1/h3/cells`
  - `GET /api/v1/h3/heatmap`
  - `GET /api/v1/h3/cells/{h3_cell}`

### Cards And KPIs

Total Zones:
- Shows number of zones in the current scored dataset.
- Expected current implementation target: 55 Singapore planning areas.

High Risk:
- Count and percentage of high-risk zones.
- Purpose: severity concentration.

Medium Risk:
- Count and percentage of medium-risk zones.
- Purpose: watchlist pressure.

Low Risk:
- Count and percentage of low-risk zones.
- Purpose: healthy coverage context.

Average Risk Score:
- Mean risk score across all zones.
- Includes sparkline proxy from high-risk trend.
- Purpose: citywide risk level independent of individual outliers.

### Main Panels

Zone Table:
- Searchable, filterable table of zones.
- Filters: risk level, region, text search.
- Sort: risk score descending by default.
- Columns include zone, region, risk score, supply, depletion rate, and recommendation context.
- Supports CSV export of the filtered zone list.

Risk Level Panel:
- Shows high/medium/low distribution and selected zone summary.
- Purpose: provides quick context while browsing zone rows.

Selected Zone Detail:
- Appears after a zone is selected.
- Shows zone name, region, risk badge, depletion risk score, key drivers, supply signals, and recommended action.
- Supply signals include current supply, one-hour depletion rate, and supply versus yesterday.
- Key drivers are parsed from the backend explanation tag, such as rapid depletion, rain, peak hour, low supply, or normal conditions.

Key Insight:
- Plain-language summary of whether risk is concentrated or broadly stable.
- Purpose: helps ops distinguish widespread stress from isolated local issues.

Risk Distribution:
- Donut chart for high, medium, and low zone counts.
- Shows concentration percentage for high-risk zones.

Pipeline Status:
- Shows last run state, PSI, flagged zones, failed rows, and scoring latency.
- Purpose: lets the analyst judge whether the zone list is fresh and reliable.

Acceptance criteria:
- User can search or filter to a zone and understand its risk drivers within 2 minutes.
- User can export the currently filtered zone set.
- Empty state instructs the user to run scoring when no zone scores exist.
- H3 mode shows an explicit empty state when `h3_predictions.csv` has not been generated.

## 8. Dashboard 3: Action Center

Route: `/actions`

Question answered: What should ops recommend, and how should follow-through be recorded?

Primary endpoint dependencies:
- `GET /api/v1/recommendations`
- `POST /api/v1/recommendations/{recommendation_id}/feedback`
- `GET /api/v1/reports/outcomes` invalidated after feedback.

### Cards And KPIs

Actions in Queue:
- Count of recommendations currently available.
- Secondary text shows critical and high-priority count.
- Purpose: workload indicator.

Average Depletion Risk:
- Mean risk score across the recommendation queue.
- Purpose: severity level of current action backlog.

Average Confidence:
- Mean recommendation confidence.
- Shows min/max confidence when available.
- Purpose: helps distinguish strong recommendations from weak or sparse evidence.

Priority Tabs:
- All, Critical, High, Medium, Low.
- Counts per priority.
- Purpose: triage by urgency.

### Recommendation Queue

Each recommendation row shows:
- Rank.
- Priority badge.
- Zone and issue detected.
- Recommended action text.
- Expected improvement or expected impact.
- Estimated cost.
- ROI/opportunity ratio when available.
- Confidence gauge.

Filtering and sorting:
- Search by zone or issue.
- Filter by risk score range.
- Sort by risk descending, risk ascending, zone name, or confidence descending.

### Detail Panel

Header:
- Priority badge, zone, issue, and depletion risk score.

Key Drivers:
- Driver tags from explanation text.
- Purpose: explains why the recommendation exists.

Expected Impact:
- Recovery probability.
- Action priority.
- Estimated cost.
- Supply response in 30 minutes.
- Confidence bar.

Learned Policy Signal:
- Recovery rate, improve/recover rate, evidence count, confidence band, follow-through rate, and policy-rank reason when historical outcomes exist.
- Purpose: starts closing the loop from recommendation to learned policy behavior.

Projected Trajectory:
- Illustrative no-action versus with-action risk trajectory.
- Explicitly heuristic, not a guarantee of real-world outcome.

Recommended Action Box:
- Full human-readable action.
- Shows decision objective and constraints triggered.

Diagnostics:
- Root cause, time to critical, action window, network warning, and alternative actions when available.

Action Buttons:
- View Details: opens a toast summary.
- Mark Followed: records that ops followed the recommendation.
- Not Followed: records that ops did not follow the recommendation.

Governance fields now available from backend:
- `action_tier`
- `action_tier_label`
- `requires_approver`
- `trigger_condition`
- `expected_supply_uplift`
- `estimated_recoverable_opportunity`
- `opportunity_ratio`

Acceptance criteria:
- Every recommendation has a priority, confidence, cost, action text, expected supply effect, and governance tier.
- No UI copy should imply the system directly launches actions.
- Followed/not-followed feedback persists to the outcome log.

## 9. Dashboard 4: Model Health

Route: `/health`

Question answered: Are the model and pipeline safe to trust, and how does the current champion compare with older model versions?

Primary endpoint dependencies:
- `GET /api/v1/model/status`
- `GET /api/v1/model/versions`
- `GET /api/v1/pipeline/latest-run`
- `GET /api/v1/monitoring/drift`
- `GET /api/v1/monitoring/history`
- `GET /api/v1/alerts`
- `POST /api/v1/pipeline/retrain`

### Controls

Model selector:
- Lists trained model versions.
- Distinguishes production and candidate versions.

Metric selector:
- Lets the user switch the performance trend between classification metrics and error/regression-style metrics.
- Supported metrics depend on what is present in the registry; current training emits ROC AUC, F1, precision, recall, MAE, RMSE, MSE, R2, log loss, Brier score, best threshold, prediction mean/std, positive rate, and row counts.

Model comparison metric selector:
- Chooses the metric used to calculate model-version delta against the current baseline/champion.
- Higher-is-better metrics and lower-is-better metrics are handled with the correct delta direction.

Retrain Model:
- Starts retraining through the backend.
- Shows toast state for start/success/error.
- Invalidates model and history queries after start.

Run history range:
- Slider chooses number of recent runs to display.
- Quick action to view all available recent runs.

### Cards And KPIs

ROC AUC:
- Active model ROC AUC from registry.
- Shows version sparkline when multiple versions exist.
- Purpose: overall ranking quality across thresholds.

F1:
- Active model F1 at the selected validation threshold.
- Purpose: balance between precision and recall for imbalanced depletion events.

Precision:
- Active model precision.
- Purpose: of alerts fired, how many were genuine risk signals.

Recall:
- Active model recall.
- Purpose: of genuine depletion events, how many the model caught.

RMSE:
- Probability-error RMSE computed against the binary depletion target.
- Purpose: regression-style measure of whether predicted probabilities are numerically close to observed outcomes.

PSI (Drift):
- Current population stability index.
- Badges: stable, warning, drift detected.
- Thresholds: warning at 0.10, alert at 0.25.

Last Trained:
- Timestamp of last retraining.
- Shows success/never state.

### Main Panels

Data & Prediction Drift:
- PSI circular gauge.
- Score distribution shift with reference/current mean and standard deviation.
- Reference and current sample counts when available.
- Per-feature drift when present.
- Purpose: detects whether current inputs or scores have moved from training/reference behavior.

Performance Trend:
- Metric trend across model versions.
- Supports classification metrics such as ROC AUC, F1, precision, and recall.
- Supports probability/regression-style metrics such as MAE, RMSE, log loss, Brier score, and R2 when available.
- Shows target line for metrics with configured targets.
- Purpose: MLflow-like comparison of how model quality changes by version, without hard-coding the view to AUC.

Model Version Comparison:
- MLflow-style comparison table of model versions.
- Columns include version ID, status, trained timestamp, ROC AUC, F1, precision, recall, MAE, RMSE, and selected metric delta.
- Delta is computed against the active/baseline version.
- Purpose: compare champion, candidate, and previous models side by side.

Operational Run History:
- Table of recent scoring/training/system runs.
- Columns: run ID, timestamp, active taxis, PSI, flagged zones, latency, status, action.
- View Report button opens run summary toast.
- Purpose: operational pipeline history, not model experiment comparison. Model experiment comparison lives in Model Version Comparison.

Recommendation / Alerts Banner:
- Shows drift alert, rollback alert, validation failure alert, or healthy state.
- Purpose: converts ML health data into operational action.

Acceptance criteria:
- User can tell whether model output is fresh, drifting, degraded, or healthy.
- User can trigger retraining from the dashboard.
- Run history does not confuse active taxi count with total rows scored.

## 10. Supporting Reporting Surface

Route: `/reports`

Reports is a supporting operational review surface for weekly learning and model-impact checks.

Tabs:
- Zone Performance: chronic high-risk zones, most improved zones, deteriorating zones over 7 or 14 days.
- Intervention Outcomes: total logged recommendations, resolved outcomes, recovery rate, improvement rate, outcomes by action type, most intervened zones, follow-through split, strongest context buckets, and recent resolved outcomes.
- Model Impact: PSI business impact, precision, recall, F1, false-positive note, and model version lineage export.

## 11. Business Logic Added From Reference Files

Implemented in the repo:
- Action governance tiers and approvers for Tier 0 through Tier 4.
- Tier 2 `driver_comms` recommendation option.
- Recommendation payload fields for tier, approver, trigger condition, expected supply uplift, estimated recoverable opportunity, and opportunity ratio.
- ROI bridge wording and calculation as illustrative recoverable opportunity rather than expected revenue.
- Outcome-log persistence for governance and ROI fields.
- Synthetic-control validity-gate helper with defaults for pre-period fit, donor pool size, spillover exclusion, placebo test, and minimum post-period.
- YAML configuration for governance, ROI assumptions, and causal validity thresholds.
- Optional H3 API and frontend drilldown path, backed by `h3_predictions.csv`.

Not yet fully implemented:
- Full `/simulate` supply recovery endpoint with uncertainty bands.
- Switchback experiment designer endpoint and UI.
- Synthetic-control result endpoint wired to real intervention outcomes.
- SHAP-based feature attribution endpoint.
- Automated H3 feature generation from live raw taxi GPS snapshots; current H3 scoring expects prepared `h3_supply_features.csv`.
- Training-serving skew dashboard separate from drift.
- Shadow-mode model promotion UI details.

## 12. API Contract Summary

Current primary frontend API:
- `GET /api/v1/overview`
- `GET /api/v1/zones`
- `GET /api/v1/zones/{zone_id}`
- `GET /api/v1/recommendations`
- `POST /api/v1/recommendations/{recommendation_id}/feedback`
- `GET /api/v1/model/status`
- `GET /api/v1/model/versions`
- `GET /api/v1/pipeline/latest-run`
- `GET /api/v1/monitoring/drift`
- `GET /api/v1/monitoring/history`
- `GET /api/v1/alerts`
- `GET /api/v1/health/services`
- `POST /api/v1/pipeline/retrain`
- `GET /api/v1/reports/zone-performance`
- `GET /api/v1/reports/outcomes`
- `GET /api/v1/reports/model-impact`
- `GET /api/v1/h3/cells`
- `GET /api/v1/h3/cells/{h3_cell}`
- `GET /api/v1/h3/heatmap`

## 13. Success Criteria

The current app is successful when:
- Project Brief explains the system and deployment model accurately.
- Overview loads and shows current market status without API errors.
- Zone Risk Monitor lists all scored zones and supports drilldown/filter/export.
- H3 mode either renders generated H3 predictions or clearly explains that H3 scoring must be run first.
- Action Center shows ranked recommendations and records follow-through.
- Model Health shows active model metrics, drift state, run history, and retraining control.
- Reports provide usable weekly review artifacts for zones, interventions, and model impact.
- Recommendation language remains governance-aware and never implies automatic dispatch, pricing, or intervention launch.
