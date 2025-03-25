from celery import Celery
from src.services.db import get_session
from src.config.settings import get_settings
from src.schemas.data_schema import AISData, TLE, Satellite
import json, tempfile, os, bz2
import urllib.request
from tqdm import tqdm
from datetime import datetime
import requests

settings = get_settings()
app = Celery("ingestion", broker="redis://localhost:6379/0")

@app.task
def ingest_dummy_data():
    with open("data.txt", "r") as f:
        data = json.load(f)

    with next(get_session()) as session:
        for i in tqdm(range(len(data))):
            ship = data[i]

            past_data = session.query(AISData).filter(AISData.mmsi == ship["MMSI"]).all()

            if past_data:
                if ship["TIME"] == past_data[-1].timestamp:
                    continue

            ais_data = AISData(
                mmsi=ship["MMSI"],
                timestamp=ship["TIME"],
                latitude=ship["LATITUDE"],
                longitude=ship["LONGITUDE"],
                cog=ship["COG"],
                sog=ship["SOG"],
                imo=ship["IMO"],
                heading=ship["ROT"],
                navstat=ship["NAVSTAT"],
                name=ship["NAME"],
                callsign=ship["CALLSIGN"],
                vessel_type=ship["TYPE"],
                a=ship["A"],
                b=ship["B"],
                c=ship["C"],
                d=ship["D"],
                draught=ship["DRAUGHT"],
                destination=ship["DEST"],
                eta=ship["ETA"]
            )

            session.add(ais_data)
            session.commit()

@app.task
def ingest_AIS_data():
    try:
        datafile = tempfile.mkstemp(prefix="aishub-data-")
        os.write(datafile[0], urllib.request.urlopen(settings.AISHUB_URL).read())

        with bz2.open(datafile[1], "rt", encoding="utf-8") as f:
            data = json.load(f)

        row_count = 0

        with next(get_session()) as session:
            #with alive_bar(len(data[1])) as bar:
                for ship in data[1]:
                    #ship = data[1][i]
                    #try:
                    row_count += 1

                    time = datetime.strptime(ship["TIME"], "%Y-%m-%d %H:%M:%S %Z")

                    ais_data = AISData(
                        mmsi=ship["MMSI"],
                        timestamp=time,
                        latitude=ship["LATITUDE"],
                        longitude=ship["LONGITUDE"],
                        cog=ship["COG"],
                        sog=ship["SOG"],
                        imo=ship["IMO"],
                        heading=ship["ROT"],
                        navstat=ship["NAVSTAT"],
                        name=ship["NAME"],
                        callsign=ship["CALLSIGN"],
                        vessel_type=ship["TYPE"],
                        a=ship["A"],
                        b=ship["B"],
                        c=ship["C"],
                        d=ship["D"],
                        draught=ship["DRAUGHT"],
                        destination=ship["DEST"],
                        eta=ship["ETA"]
                    )

                    session.add(ais_data)
                    session.commit()

        os.unlink(datafile[1])

    except Exception as e:
        print(f"Error: {e}")

@app.task
def fetch_tles():
    with next(get_session()) as session:
        for satellite in session.query(Satellite).all():
            print(f"Fetching TLE for {satellite.name}")
            past_tles = session.query(TLE).filter(TLE.satellite_id == satellite.id).all()
            for past_tle in past_tles:
                if datetime.strptime(past_tle.created_at.split(".")[0], "%Y-%m-%d %H:%M:%S").date() == datetime.now().date():
                    print("TLE already fetched today")
                    continue
            tle = requests.get(f"https://api.n2yo.com/rest/v1/satellite/tle/{satellite.id}&apiKey={settings.N2YO_API_KEY}").json()
            line1 = tle["tle"].splitlines()[0]
            line2 = tle["tle"].splitlines()[1]
            new_tle = TLE(
                    satellite_id=satellite.id,
                    line1=line1,
                    line2=line2
                    )

            update_sat = session.query(Satellite).filter(Satellite.id == satellite.id).first()
            update_sat.tles.append(new_tle)

            session.add(update_sat)
            session.add(new_tle)
            session.commit()
