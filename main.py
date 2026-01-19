from fastapi import FastAPI, HTTPException, Request
from pydantic import BaseModel
import os
import logging
from datetime import datetime

# -------------------------------------------------------------------
# App setup
# -------------------------------------------------------------------

app = FastAPI(
    title="MMForecasting Compute Service",
    version="0.1.0",
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)

logger = logging.getLogger(__name__)

# -------------------------------------------------------------------
# Environment validation (fail fast if misconfigured)
# -------------------------------------------------------------------

REQUIRED_ENV_VARS = [
    "SUPABASE_URL",
    "SUPABASE_SERVICE_ROLE_KEY",
    "ALPACA_API_KEY",
    "ALPACA_API_SECRET",
]

missing_vars = [v for v in REQUIRED_ENV_VARS if not os.getenv(v)]
if missing_vars:
    raise RuntimeError(f"Missing required environment variables: {missing_vars}")

# -------------------------------------------------------------------
# Request / response models
# -------------------------------------------------------------------

class RunRequest(BaseModel):
    run_id: str


class RunResponse(BaseModel):
    run_id: str
    status: str
    received_at: str


# -------------------------------------------------------------------
# Health check (used by Render)
# -------------------------------------------------------------------

@app.get("/health")
def health():
    """
    Lightweight health check.
    Render uses this to determine if the service is up.
    """
    return {"status": "ok"}


# -------------------------------------------------------------------
# Compute entrypoint (called by Vercel / QStash later)
# -------------------------------------------------------------------

@app.post("/compute/run", response_model=RunResponse)
async def run_compute_job(payload: RunRequest, request: Request):
    """
    Entry point for running a forecasting + backtest job.

    For now:
    - Validates input
    - Logs the request
    - Returns acknowledgement

    Later:
    - Load run config from Supabase using run_id
    - Execute Phases 1–8
    - Persist results back to Supabase
    """

    run_id = payload.run_id.strip()

    if not run_id:
        raise HTTPException(status_code=400, detail="run_id must be non-empty")

    logger.info(
        "Received compute job",
        extra={
            "run_id": run_id,
            "client": request.client.host if request.client else "unknown",
        },
    )

    # ----------------------------------------------------------------
    # PLACEHOLDER FOR PIPELINE EXECUTION
    # ----------------------------------------------------------------
    # Example (later):
    # config = load_run_config(run_id)
    # results = run_full_pipeline(config)
    # save_results_to_supabase(run_id, results)
    # ----------------------------------------------------------------

    return RunResponse(
        run_id=run_id,
        status="accepted",
        received_at=datetime.utcnow().isoformat() + "Z",
    )