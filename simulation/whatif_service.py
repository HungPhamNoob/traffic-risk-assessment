from simulation.engine import recalculate_risk


def run_scenario(  # noqa: E501
    base_risk: float, speed_factor: float, weather_factor: float
) -> dict[str, float]:  # noqa E125
    return {"adjusted_risk": recalculate_risk(base_risk, speed_factor, weather_factor)}
