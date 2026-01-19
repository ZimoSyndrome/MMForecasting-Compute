from fastapi import FastAPI, HTTPException, Request
from pydantic import BaseModel
import os
import logging
from datetime import datetime, timezone, timedelta
import traceback
import pandas as pd
from supabase import create_client
# --- NEW IMPORTS ---
from app.services.data.ingestion import DataIngestion
from app.services.data.processing import DataProcessor
# -------------------
app = FastAPI(title="MMForecasting Compute Service", version="0.3.0")
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)
# Ensure these match what Ingestion expects
REQUIRED_ENV_VARS = [
    "SUPABASE_URL",
    "SUPABASE_SERVICE_ROLE_KEY",
    # "ALPACA_API_KEY", # Optional if using Yahoo
    # "ALPACA_API_SECRET",
]
missing_vars = [v for v in REQUIRED_ENV_VARS if not os.getenv(v)]
if missing_vars:
    # Just warn for now to allow local testing if needed
    logger.warning(f"Missing environment variables: {missing_vars}")
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
    logger.info(f"Received compute job run_id={run_id}")
    sb = supabase()
    try:
        # 1) Mark Running
        sb.table("runs").update({
            "status": "running", 
            "progress_step": "fetching_data",
            "progress_pct": 10
        }).eq("run_id", run_id).execute()
        # 2) Load Config
        run_row = sb.table("runs").select("*").eq("run_id", run_id).single().execute()
        if not run_row.data:
            raise RuntimeError("run_id not found")
        
        config = run_row.data
        ticker = config["ticker"]
        start_date = config["start_date"]
        end_date = config["end_date"]
        # Use Yahoo as default fallback if Alpaca keys missing
        source = "alpaca" if os.getenv("ALPACA_API_KEY") else "yahoo"
        # 3) REAL PIPELINE: Ingestion
        ingest = DataIngestion()
        df_raw = ingest.fetch_data(ticker, start_date, end_date, source=source)
        
        if df_raw.empty:
            raise RuntimeError(f"No data found for {ticker} via {source}")
        sb.table("runs").update({
            "progress_step": "processing_data",
            "progress_pct": 20
        }).eq("run_id", run_id).execute()
        # 4) REAL PIPELINE: Processing
        processor = DataProcessor()
        df_clean = processor.clean_and_process(df_raw, ticker)
        
        if df_clean.empty:
            raise RuntimeError(f"Data processing resulted in empty set for {ticker}")
        # 5) Write ACTUALS to Database
        # We write the 'actual_return' so the chart has a ground truth line.
        # Use bulk upsert for efficiency.
        
        # Convert DataFrame to list of dicts
        ts_rows = []
        for _, row in df_clean.iterrows():
            ts_rows.append({
                "run_id": run_id,
                "model": "actual", # Special tag for ground truth
                "ts": row['date'].isoformat(),
                "actual_return": row['log_return'],
                "pred_return": None, # No predictions yet
                "equity_curve": None
            })
            
        # Write in chunks if large (supa limit)
        chunk_size = 1000
        for i in range(0, len(ts_rows), chunk_size):
            sb.table("run_timeseries").upsert(ts_rows[i:i+chunk_size]).execute()
        # 6) Mark Complete (Phase 1 Done)
        sb.table("runs").update({
            "status": "complete",
            "progress_step": "done",
            "progress_pct": 100
        }).eq("run_id", run_id).execute()
        return RunResponse(run_id=run_id, status="complete", received_at=utc_now_iso())
    except Exception as e:
        err = f"{type(e).__name__}: {str(e)}"
        logger.error(traceback.format_exc())
        sb.table("runs").update({
            "status": "failed",
            "error_message": err
        }).eq("run_id", run_id).execute()
        raise HTTPException(status_code=500, detail=err)