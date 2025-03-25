from celery import Celery
from datetime import datetime, timedelta
from src.services.db import get_session
from src.schemas.data_schema import Vessel, VesselStatus, SatPass, TLE, Satellite, AISData
from src.services.calculations import get_closest_pass, add_distance_to_gps
from src.services.inference import *
from src.config.settings import get_settings
import numpy as np
import math
from tqdm import tqdm
import logging


settings = get_settings()
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("alive_progress")

app = Celery('tasks', broker='redis://localhost:6379/0')

@app.task(bind=True, max_retries=3, default_retry_delay=30)
def process_passes(self):
    pass 


def is_data_valid(data):
    pass 

@app.task(bind=True, max_retries=3, default_retry_delay=30)
def process_vessel_data(self):
    """
    Process AIS data and parse into Vessel and VesselStatus
    """

    with next(get_session()) as session:
        ais_data = session.query(AISData).all()
        
        print(len(ais_data))

        for data in ais_data:
            print(f"Processing vessel data for {data.name}")
            vessel = session.query(Vessel).filter(Vessel.imo == data.imo).first()

            if not vessel:
                vessel = Vessel(
                    imo=data.imo,
                    mmsi = data.mmsi,
                    vessel_name=data.name,
                    vessel_type=data.vessel_type,
                    vesselfinder_url=None,
                    flag=None,
                    length=data.a + data.b,
                    beam=data.c + data.d,
                    year_built=None
                    )

                status = VesselStatus(
                    imo=data.imo,
                    freshness=data.timestamp,
                    latitude=data.latitude,
                    longitude=data.longitude,
                    speed=data.sog,
                    course=data.cog,
                    status=data.navstat,
                    tonnage=None,
                    draught=data.draught
                    )

            elif datetime.strptime(data.timestamp, "%Y-%m-%d %H:%M:%S") < datetime.strptime(vessel.statuses[-1].freshness, "%Y-%m-%d %H:%M:%S"):
                status = VesselStatus(
                    imo=data.imo,
                    freshness=data.timestamp,
                    latitude=data.latitude,
                    longitude=data.longitude,
                    speed=data.sog,
                    course=data.cog,
                    status=data.navstat,
                    tonnage=None,
                    draught=data.draught
                    )

            session.add(vessel)
            session.commit()

            vessel.statuses.append(status)
            session.add(status)


