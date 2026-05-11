"""
Model Evaluator
===============
Runs evaluation on a held-out test set and produces a metrics report.
Used both during training and during the promotion gate.

Metrics computed:
    Classification : accuracy, balanced accuracy, precision, recall, f1, roc_auc, confusion matrix
    Probability    : log loss, Brier score, MAE, RMSE, MSE, R2 over predicted probabilities
    Calibration    : ECE (expected calibration error), reliability bins (fraction_positive vs mean_predicted)
    Stability      : prediction variance, % predictions above 0.5 threshold

Baseline comparison:
    Compares candidate model metrics against the currently active model
    metrics stored in the registry.
"""

import numpy as np
import pandas as pd
from sklearn.calibration import calibration_curve
from sklearn.metrics import (
    precision_score, recall_score, f1_score, roc_auc_score,
    mean_absolute_error, mean_squared_error, confusion_matrix,
    accuracy_score, balanced_accuracy_score, log_loss, brier_score_loss,
    r2_score,
)


def evaluate(model, X_test: pd.DataFrame, y_test: pd.Series) -> dict:
    """
    Full evaluation report for a trained model.

    F1 is reported at the optimal threshold (maximises F1 on the val set)
    rather than the default 0.5. This is standard practice for imbalanced
    binary classification — the supply shortage target is ~20% positive,
    so the default threshold systematically hurts either precision or recall.
    """
    from sklearn.metrics import precision_recall_curve

    y_prob = model.predict_proba(X_test)[:, 1]

    # Find threshold that maximises F1 on this validation set
    precision_curve, recall_curve, thresholds = precision_recall_curve(y_test, y_prob)
    f1_curve = np.where(
        (precision_curve + recall_curve) > 0,
        2 * precision_curve * recall_curve / (precision_curve + recall_curve),
        0,
    )
    best_idx = int(np.argmax(f1_curve[:-1]))  # last element has no threshold
    best_threshold = float(thresholds[best_idx])
    y_pred = (y_prob >= best_threshold).astype(int)

    cm = confusion_matrix(y_test, y_pred).tolist()

    # Calibration: expected calibration error and reliability diagram bins.
    # ECE = mean |fraction_of_positives - mean_predicted_prob| across bins.
    # A well-calibrated model has ECE < 0.05; > 0.10 means systematic over-
    # or under-confidence and suggests recalibration is needed.
    frac_pos, mean_pred = calibration_curve(
        y_test, y_prob, n_bins=10, strategy="uniform"
    )
    ece = round(float(np.mean(np.abs(frac_pos - mean_pred))), 4)
    reliability_bins = [
        {"bin_center": round(float(mp), 3), "fraction_positive": round(float(fp), 3)}
        for mp, fp in zip(mean_pred, frac_pos)
    ]

    metrics = {
        "accuracy":          round(float(accuracy_score(y_test, y_pred)), 4),
        "balanced_accuracy": round(float(balanced_accuracy_score(y_test, y_pred)), 4),
        "precision":         round(float(precision_score(y_test, y_pred, zero_division=0)), 4),
        "recall":            round(float(recall_score(y_test, y_pred, zero_division=0)), 4),
        "f1":                round(float(f1_score(y_test, y_pred, zero_division=0)), 4),
        "roc_auc":           round(float(roc_auc_score(y_test, y_prob)), 4),
        "log_loss":          round(float(log_loss(y_test, y_prob, labels=[0, 1])), 4),
        "brier_score":       round(float(brier_score_loss(y_test, y_prob)), 4),
        "ece":               ece,
        "reliability_bins":  reliability_bins,
        "best_threshold":    round(best_threshold, 4),
        "mae":               round(float(mean_absolute_error(y_test, y_prob)), 4),
        "rmse":              round(float(np.sqrt(mean_squared_error(y_test, y_prob))), 4),
        "mse":               round(float(mean_squared_error(y_test, y_prob)), 4),
        "r2":                round(float(r2_score(y_test, y_prob)), 4),
        "pred_mean":         round(float(y_prob.mean()), 4),
        "pred_std":          round(float(y_prob.std()), 4),
        "pct_above_threshold": round(float((y_prob >= best_threshold).mean()), 4),
        "confusion_matrix":  cm,
        "n_test":            int(len(y_test)),
        "positive_rate":     round(float(y_test.mean()), 4),
    }
    return metrics


def compare_to_active(candidate_metrics: dict, active_metrics: dict) -> dict:
    """Return comparison dict with delta values and pass/fail flags.

    A metric regresses if it drops more than 5% relative to the active value.
    """
    compare_keys = ["precision", "recall", "f1", "roc_auc"]
    result = {}
    all_pass = True
    for key in compare_keys:
        cand  = candidate_metrics.get(key, 0.0)
        actv  = active_metrics.get(key, 0.0)
        delta = cand - actv
        # Regression = candidate is more than 5% worse (relative)
        regressed = (actv > 0) and (delta / actv < -0.05)
        if regressed:
            all_pass = False
        result[key] = {
            "candidate":  cand,
            "active":     actv,
            "delta":      round(delta, 4),
            "regressed":  regressed,
        }
    result["all_pass"] = all_pass
    return result
