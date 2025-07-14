from fastapi import APIRouter

router = APIRouter(prefix="/lineage", tags=["lineage"])

@router.get("/{column}")
def get_lineage(column: str):
    return {"column": column, "path": []}   # placeholder