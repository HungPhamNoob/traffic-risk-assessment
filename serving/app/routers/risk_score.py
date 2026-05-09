from fastapi import APIRouter
from pydantic import BaseModel

router = APIRouter()


class RiskScoreRequest(BaseModel):
    lat: float
    lon: float


@router.post("/")
def get_risk_score(payload: RiskScoreRequest) -> dict[str, float]:
    # Placeholder score for initial scaffold.
    return {"lat": payload.lat, "lon": payload.lon, "risk_score": 0.5}
