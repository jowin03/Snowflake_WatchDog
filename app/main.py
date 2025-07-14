from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from app.api.v1 import risks, events, lineage

app = FastAPI(title="Snowwatch", version="1.0.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_headers=["*"],
    allow_methods=["*"],
)

app.include_router(risks.router)
app.include_router(events.router)
app.include_router(lineage.router)

@app.get("/")
def root():
    return {"message": "Snowwatch running 🎿"}