from apscheduler.schedulers.background import BackgroundScheduler
from contextlib import asynccontextmanager
from fastapi import FastAPI
from src.services.db import init_db
from src.routers import ingestion, status
from src.tasks.task_queue import process_vessel_data, process_passes
from src.services.ingestion import ingest_AIS_data, fetch_tles, ingest_dummy_data
import pandas as pd
import uvicorn
from datetime import datetime, timedelta
#from alive_progress import alive_bar
import time

@asynccontextmanager
async def lifespan(app: FastAPI):
    '''
    Context manager to handle the lifespan of the FastAPI app

    Args:
        app (FastAPI): FastAPI app instance
    '''
    init_db()

    # Background scheduler
    scheduler = BackgroundScheduler()
    # Schedule the job
    scheduler.add_job(ingest_AIS_data, "interval", minutes=30, next_run_time=datetime.now()) # This looks fine 
    scheduler.add_job(process_vessel_data, "interval", minutes=30, next_run_time=datetime.now() + timedelta(minutes=2)) # This looks fine
    scheduler.add_job(process_passes, "interval", minutes=30, next_run_time=datetime.now() + timedelta(minutes=4)) # TODO The code for this from the eofusion repo looks odd with typos and missing code
    scheduler.add_job(fetch_tles, "interval", days=1, next_run_time=datetime.now()) # This one looks fine 

    # Start the scheduler
    scheduler.start()

    yield
    scheduler.shutdown()

# FastAPI app instance
app = FastAPI(title="Processing API", lifespan=lifespan)

# Include routers
app.include_router(ingestion.router, prefix="/ingest", tags=["Ingestion"])
app.include_router(status.router, prefix="/status", tags=["Status"])


@app.get("/")
async def read_root():
    return {"message": "Processing API is running"}


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)