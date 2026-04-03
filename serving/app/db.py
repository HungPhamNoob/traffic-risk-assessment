import os


def get_postgres_dsn() -> str:
	host = os.getenv("POSTGRES_HOST", "localhost")
	port = os.getenv("POSTGRES_PORT", "5432")
	user = os.getenv("POSTGRES_USER", "postgres")
	password = os.getenv("POSTGRES_PASSWORD", "changeme")
	dbname = os.getenv("POSTGRES_DB", "accident_risk")
	return f"postgresql://{user}:{password}@{host}:{port}/{dbname}"

