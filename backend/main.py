"""FraudFish API — FastAPI application entry point."""

import sys
import os

sys.path.insert(0, os.path.dirname(__file__))

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from api.investigate import router as investigate_router
from api.scan import router as scan_router
from api.dashboard import router as dashboard_router

app = FastAPI(title="FraudFish API", version="0.1.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:5173"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(investigate_router)
app.include_router(scan_router)
app.include_router(dashboard_router)


@app.get("/api/health")
async def health():
    return {"status": "ok"}
