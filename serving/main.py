from fastapi import FastAPI

from serving.app.routers.hotspots import router as hotspots_router
from serving.app.routers.risk_score import router as risk_router
from serving.app.routers.whatif import router as whatif_router

app = FastAPI(title="Road Accident Risk API", version="0.1.0")

app.include_router(hotspots_router, prefix="/hotspots", tags=["hotspots"])
app.include_router(risk_router, prefix="/risk-score", tags=["risk-score"])
app.include_router(whatif_router, prefix="/whatif", tags=["whatif"])


@app.get("/health")
def health_check() -> dict[str, str]:
	return {"status": "ok"}

