"""FastAPI entrypoint for the Traffic Risk Assessment dashboard backend."""

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from app.routes import (
    analytics,
    hotspots,
    model,
    overview,
    predictions,
    scenarios,
    system,
)


app = FastAPI(
    title="Traffic Risk Assessment API",
    version="1.0.0",
    description="Backend API for realtime traffic risk predictions and pipeline status.",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:3000", "http://localhost:8000"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.get("/health")
def health() -> dict[str, str]:
    """Return a lightweight process health response for load balancers and CI smoke tests."""
    return {"status": "ok"}


app.include_router(overview.router, prefix="/api/v1/overview", tags=["overview"])
app.include_router(
    predictions.router, prefix="/api/v1/predictions", tags=["predictions"]
)
app.include_router(hotspots.router, prefix="/api/v1/hotspots", tags=["hotspots"])
app.include_router(scenarios.router, prefix="/api/v1/scenarios", tags=["scenarios"])
app.include_router(analytics.router, prefix="/api/v1/analytics", tags=["analytics"])
app.include_router(system.router, prefix="/api/v1/system", tags=["system"])
app.include_router(model.router, prefix="/api/v1/model", tags=["model"])
