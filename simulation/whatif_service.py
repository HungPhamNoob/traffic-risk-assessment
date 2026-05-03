from simulation.engine import recalculate_risk


def run_scenario(base_risk: float, speed_factor: float, weather_factor: float) -> dict[str, float]:
    return {"adjusted_risk": recalculate_risk(base_risk, speed_factor, weather_factor)}
