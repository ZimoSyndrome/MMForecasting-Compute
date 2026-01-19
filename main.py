from fastapi import FastAPI, HTTPException, Request
from pydantic import BaseModel
import os
import logging
from datetime import datetime, timezone, timedelta
import traceback

from supabase import create_client

app = FastAPI(title="MMForecasting Compute Service", version="0.2.0")

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

REQUIRED_ENV_VARS = [
    "SUPABASE_URL",
    "SUPABASE_SERVICE_ROLE_KEY",
    "ALPACA_API_KEY",
    "ALPACA_API_SECRET",
]

missing_vars = [v for v in REQUIRED_ENV_VARS if not os.getenv(v)]
if missing_vars:
    raise RuntimeError(f"Missing required environment variables: {missing_vars}")

def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()

def supabase():
    return create_client(os.environ["SUPABASE_URL"], os.environ["SUPABASE_SERVICE_ROLE_KEY"])

class RunRequest(BaseModel):
    run_id: str

class RunResponse(BaseModel):
    run_id: str
    status: str
    received_at: str

@app.get("/health")
def health():
    return {"status": "ok"}

@app.post("/compute/run", response_model=RunResponse)
async def run_compute_job(payload: RunRequest, request: Request):
    run_id = payload.run_id.strip()
    if not run_id:
        raise HTTPException(status_code=400, detail="run_id must be non-empty")

    logger.info(f"Received compute job run_id={run_id} from={request.client.host if request.client else 'unknown'}")

    sb = supabase()

    try:
        # 1) Mark as running
        sb.table("runs").update({
            "status": "running",
            "progress_step": "starting",
            "progress_pct": 5,
            "error_message": None
        }).eq("run_id", run_id).execute()

        # 2) Load config (ticker/date range/etc.)
        run_row = sb.table("runs").select("ticker,start_date,end_date,frequency,config").eq("run_id", run_id).single().execute()
        if not run_row.data:
            raise RuntimeError("run_id not found in runs table")

        ticker = run_row.data["ticker"]
        start_date = run_row.data["start_date"]
        end_date = run_row.data["end_date"]
        frequency = run_row.data.get("frequency", "1D")

        sb.table("runs").update({
            "progress_step": "loaded_config",
            "progress_pct": 15
        }).eq("run_id", run_id).execute()

        # 3) Placeholder "compute" (write dummy outputs so UI can render)
        # Replace this later with your real pipeline.
        sb.table("runs").update({
            "progress_step": "writing_results",
            "progress_pct": 80
        }).eq("run_id", run_id).execute()

        # 3a) Write per-model metrics into run_results
        # NOTE: your run_results schema has separate columns (mae, mse, sharpe, etc.)
        # so we insert those columns directly (not a metrics json blob).
        models = ["arima_garch", "xgboost", "lstm"]
        for m in models:
            sb.table("run_results").upsert({
                "run_id": run_id,
                "model": m,
                "mae": 0.0,
                "mse": 0.0,
                "ann_return": 0.0,
                "ann_vol": 0.0,
                "sharpe": 0.0,
                "max_drawdown": 0.0,
                "var_95": 0.0,
                "cvar_95": 0.0,
                "metadata": {
                    "note": "placeholder output",
                    "ticker": ticker,
                    "start_date": str(start_date),
                    "end_date": str(end_date),
                    "frequency": frequency,
                }
            }).execute()

        # 3b) Write a few timeseries rows (optional but useful to prove chart path)
        # Your run_timeseries schema stores one row per timestamp with multiple numeric columns.
        # Insert ~5 fake points.
        base_ts = datetime.now(timezone.utc)
        ts_rows = []

        for i in range(5):
            ts_rows.append({
                "run_id": run_id,
                "model": "arima_garch",
                "ts": (base_ts + timedelta(days=i)).isoformat(),  # UNIQUE ts
                "actual_return": 0.0,
                "pred_return": 0.0,
                "pred_vol": 0.0,
                "position": 0.0,
                "strategy_return": 0.0,
                "equity_curve": 1.0,
                "drawdown": 0.0,
            })
        sb.table("run_timeseries").upsert(ts_rows).execute()

        # 4) Mark complete
        sb.table("runs").update({
            "status": "complete",
            "progress_step": "done",
            "progress_pct": 100
        }).eq("run_id", run_id).execute()

        return RunResponse(run_id=run_id, status="complete", received_at=utc_now_iso())

    except Exception as e:
        err = f"{type(e).__name__}: {str(e)}"
        logger.error(err)
        logger.error(traceback.format_exc())

        # Mark failed
        try:
            sb.table("runs").update({
                "status": "failed",
                "progress_step": "error",
                "progress_pct": 100,
                "error_message": err
            }).eq("run_id", run_id).execute()
        except Exception:
            pass

        raise HTTPException(status_code=500, detail=err)