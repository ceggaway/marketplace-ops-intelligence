"""
Ops AI Chat Router
==================
POST /ai/chat — takes a user message + conversation history, injects
live ops context (zones, KPIs, recommendations, drift) and calls the
configured AI provider.

Set ANTHROPIC_API_KEY in the environment to enable real responses.
Without a key the endpoint returns the assembled context so the
frontend can show something useful in development.
"""

import json
import os
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
from fastapi import APIRouter
from pydantic import BaseModel

router  = APIRouter()
OUT_DIR = Path("data/outputs")


# ── Request / Response ────────────────────────────────────────────────────────

class ChatMessage(BaseModel):
    role: str   # "user" | "assistant"
    content: str

class ChatRequest(BaseModel):
    message: str
    history: list[ChatMessage] = []

class ChatResponse(BaseModel):
    reply: str


# ── Context builder ───────────────────────────────────────────────────────────

def _build_context() -> str:
    """Read flat files and assemble an ops-state summary for the AI prompt."""
    lines: list[str] = []
    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    lines.append(f"LIVE OPS CONTEXT  (as of {now})")
    lines.append("=" * 56)

    # ── Zone snapshot ─────────────────────────────────────────
    try:
        preds = pd.read_csv(OUT_DIR / "predictions.csv")
        if "timestamp" in preds.columns and not preds.empty:
            preds = preds[preds["timestamp"] == preds["timestamp"].max()]

        high   = len(preds[preds["risk_level"] == "high"])
        medium = len(preds[preds["risk_level"] == "medium"])
        low    = len(preds[preds["risk_level"] == "low"])
        total  = int(preds["taxi_count"].sum()) if "taxi_count" in preds.columns else "N/A"
        rapid  = int((preds["depletion_rate_1h"] > 0.30).sum()) if "depletion_rate_1h" in preds.columns else 0

        lines.append(f"\nZONE RISK SUMMARY  (55 zones total)")
        lines.append(f"  High risk   : {high} zones  (depletion probability ≥ 0.70)")
        lines.append(f"  Medium risk : {medium} zones  (0.40 – 0.69)")
        lines.append(f"  Low risk    : {low} zones  (< 0.40)")
        lines.append(f"  Total taxis : {total} active across all zones")
        lines.append(f"  Rapid depletion (>30%/hr): {rapid} zones")

        # Top 5 highest-risk zones
        top = preds.nlargest(5, "delay_risk_score")
        lines.append(f"\nTOP HIGH-RISK ZONES")
        for _, row in top.iterrows():
            dep = row.get("depletion_rate_1h", 0)
            dep_str = f"{dep*100:.1f}%/hr" if dep else "—"
            lines.append(
                f"  {row['zone_name']:<22} score={row['delay_risk_score']:.3f}"
                f"  level={row['risk_level']:<6}  depletion={dep_str}"
            )
    except Exception as exc:
        lines.append(f"\n[Zone data unavailable: {exc}]")

    # ── Recommendations ───────────────────────────────────────
    try:
        recs = pd.read_csv(OUT_DIR / "recommended_actions.csv")
        if "timestamp" in recs.columns and not recs.empty:
            recs = recs[recs["timestamp"] == recs["timestamp"].max()]

        crit  = len(recs[recs["priority"] == "critical"])
        high_p = len(recs[recs["priority"] == "high"])
        lines.append(f"\nACTIONS QUEUE  ({len(recs)} total, {crit} critical, {high_p} high)")
        urgent = recs[recs["priority"].isin(["critical", "high"])].head(5)
        for _, row in urgent.iterrows():
            lines.append(
                f"  [{row['priority'].upper():<8}] {row.get('zone_name','?'):<22}"
                f"  {row.get('recommendation','')[:70]}"
            )
    except Exception as exc:
        lines.append(f"\n[Recommendations unavailable: {exc}]")

    # ── Model / drift ─────────────────────────────────────────
    try:
        drift_path = OUT_DIR / "drift_report.json"
        if drift_path.exists():
            d = json.loads(drift_path.read_text())
            lines.append(
                f"\nMODEL HEALTH"
                f"\n  PSI={d.get('psi',0):.4f}  level={d.get('drift_level','unknown')}"
                f"  drift_flag={d.get('drift_flag',False)}"
            )
    except Exception:
        pass

    lines.append("\n" + "=" * 56)
    return "\n".join(lines)


SYSTEM_PROMPT = """\
You are Ops AI, an intelligent assistant for the Singapore taxi marketplace \
operations team. You have access to real-time data from the monitoring system \
shown below. Answer questions concisely and clearly. Focus on actionable \
insights — what the ops team should do RIGHT NOW. When citing zones or scores \
always use the exact numbers from the context. Keep replies under 200 words \
unless the user asks for more detail.\
"""


# ── Endpoint ──────────────────────────────────────────────────────────────────

@router.post("/ai/chat", response_model=ChatResponse)
def chat(req: ChatRequest) -> ChatResponse:
    context  = _build_context()
    api_key  = os.environ.get("ANTHROPIC_API_KEY", "").strip()

    if not api_key:
        # No key yet — return the context so the dev can verify it's wired up
        return ChatResponse(reply=(
            "**Ops AI is not configured yet.**\n\n"
            "Set `ANTHROPIC_API_KEY` in your environment to enable AI responses.\n\n"
            "Here is the live ops context I would send to the model:\n\n"
            "```\n" + context + "\n```"
        ))

    # ── Claude API call ───────────────────────────────────────
    try:
        import anthropic

        client = anthropic.Anthropic(api_key=api_key)

        # Build messages: system context injected as first user turn so it
        # stays fresh regardless of how long the history is.
        messages = []
        # Inject context at the top
        messages.append({
            "role": "user",
            "content": f"[LIVE OPS CONTEXT]\n{context}\n\n[QUESTION]\n{req.message}",
        })
        # Append prior conversation (skip the first fake system turn if history exists)
        # For subsequent turns, history carries the real back-and-forth
        if req.history:
            # Rebuild: context only in first message, then alternating turns
            messages = []
            for i, msg in enumerate(req.history):
                content = msg.content
                if i == 0 and msg.role == "user":
                    content = f"[LIVE OPS CONTEXT]\n{context}\n\n[QUESTION]\n{content}"
                messages.append({"role": msg.role, "content": content})
            messages.append({"role": "user", "content": req.message})

        ai_model = os.environ.get("AI_MODEL", "claude-haiku-4-5-20251001")
        response = client.messages.create(
            model=ai_model,
            max_tokens=512,
            system=SYSTEM_PROMPT,
            messages=messages,
        )
        reply = response.content[0].text
    except ImportError:
        reply = (
            "The `anthropic` Python package is not installed.\n"
            "Run: `pip install anthropic` then restart the API."
        )
    except Exception as exc:
        reply = f"AI call failed: {exc}"

    return ChatResponse(reply=reply)
