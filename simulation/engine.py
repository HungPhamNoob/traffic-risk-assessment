def recalculate_risk(base_risk: float, speed_factor: float, weather_factor: float) -> float:
	value = base_risk * speed_factor * weather_factor
	return max(0.0, min(1.0, value))

