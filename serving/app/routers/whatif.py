from fastapi import APIRouter
from pydantic import BaseModel

router = APIRouter()


class WhatIfRequest(BaseModel):
    base_risk: float
    weather_factor: float = 1.0
    speed_factor: float = 1.0


@router.post("/")
def run_whatif(payload: WhatIfRequest) -> dict[str, float]:
    adjusted = payload.base_risk * payload.weather_factor * payload.speed_factor
    return {"adjusted_risk": max(0.0, min(1.0, adjusted))}
