from fastapi import APIRouter

router = APIRouter()


@router.get("/")
def list_hotspots() -> dict[str, list[dict[str, float]]]:
    return {"items": []}
