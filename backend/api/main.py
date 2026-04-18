"""
FastAPI Application Entry Point
================================
Starts the API server that the Vite/React frontend calls.

Run with:
    uvicorn backend.api.main:app --reload --port 8000

Docs available at:
    http://localhost:8000/docs   (Swagger UI)
    http://localhost:8000/redoc  (ReDoc)
"""

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from backend.api.routers import operational, ml_health, ai_chat

app = FastAPI(
    title="Marketplace Ops Intelligence API",
    description="Backend API for the operations command center dashboard.",
    version="1.0.0",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # tighten in production
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(operational.router, prefix="/api/v1", tags=["operational"])
app.include_router(ml_health.router,   prefix="/api/v1", tags=["ml-health"])
app.include_router(ai_chat.router,     prefix="/api/v1", tags=["ai-chat"])


@app.get("/health")
def health_check():
    return {"status": "ok"}
