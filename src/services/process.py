from celery import Celery
from datetime import datetime, timedelta
from src.services.db import get_session
from src.schemas.data_schema import Vessel, VesselStatus, SatPass, TLE, Satellite, AISData
from src.services.calculations import get_closest_pass, add_distance_to_gps
from src.services.inference import generate_composite_image , run_ship_detection
from src.services.inference import *
from src.config.settings import get_settings
from tqdm import tqdm
from src.logger import get_logger
import logging




settings = get_settings()
logger = get_logger(__name__)

app = Celery('tasks', broker='redis://localhost:6379/0')




def is_data_valid(data):
    pass 

@app.task(bind=True, max_retries=3, default_retry_delay=30)
def process_vessel_data(self):
    """
    Process AIS data and parse into Vessel and VesselStatus
    """

    with next(get_session()) as session:
        ais_data = session.query(AISData).all()
        
        logger.debug(f"Processing {len(ais_data)} AIS data entries")

        for data in ais_data:
            logger.debug(f"Processing vessel data for {data.name}")
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




@app.task(bind=True, max_retries=3, default_retry_delay=30)
def process_passes(self):
    with next(get_session()) as session:
        vessels = session.query(Vessel).all()

        # Check the closest passes for each vessel are up to date
        for i in tqdm(range(len(vessels))):
        #for vessel in vessels:
            vessel = vessels[i]
            logger.debug("Processing passes for {vessel.vessel_name}")
            last_pass = session.query(SatPass).filter(SatPass.status_id == vessel.statuses[-1].id).order_by(SatPass.timestamp.desc()).first()
            if last_pass:
                if datetime.strptime(last_pass.timestamp, "%Y-%m-%d %H:%M:%S %Z") > datetime.strptime(vessel.statuses[-1].freshness, "%Y-%m-%d %H:%M:%S %Z"):
                    continue

            ############################
            ##This code is Error prone##
            latitude = vessel.statuses[-1].latitude 
            longitude = vessel.statuses[-1].longitude
            search_date = vessel.statuses[-1].freshness
            ############################
            
            
            # Get the closest passes
            closest_passes = get_closest_pass(latitude, longitude, search_date, session.query(TLE).all())

            #for pass_ in closest_passes[:10]:
            for j in tqdm(range(len(closest_passes[:10]))):
                pass_ = closest_passes[j]

                lat = pass_[0][0]
                lon = pass_[0][1]

                # Calculate the bounding box
                north_lat, _ = add_distance_to_gps(lat, lon, 10, 0)
                south_lat, _ = add_distance_to_gps(lat, lon, 10, 180)
                _, east_lon = add_distance_to_gps(lat, lon, 10, 90)
                _, west_lon = add_distance_to_gps(lat, lon, 10, 270)

                bbox = (
                        f"POLYGON(("
                        f"{west_lon} {south_lat}, {east_lon}  {south_lat},"
                        f"{east_lon} {north_lat}, {west_lon} {north_lat},"
                        f"{west_lon} {south_lat}))"
                )
                logger.debug(f"Authenticating")
                # Authenticate
                access_token = authenticate(settings.AUTH_URL, settings.USERNAME, settings.PASSWORD)
                api_session = requests.Session()
                api_session.headers["Authorization"] = f"Bearer {access_token}"

                current_date = datetime.strptime(pass_[2], "%Y-%m-%d %H:%M:%S")
                end_date = current_date + timedelta(days=3)

                # Query the catalogue
                while current_date < end_date:
                    result = query_catalogue(
                            catalogue_odata_url=settings.CATALOGUE_URL,
                            collection_name=settings.COLLECTION_NAME,
                            product_type=settings.PRODUCT_TYPE,
                            aoi=bbox,
                            max_cloud_cover=100,
                            search_period_start=current_date,
                            search_period_end=current_date + timedelta(days=1),
                    )

                    # Increment the date
                    current_date += timedelta(days=1)

                    if result.empty:
                        # Skip if no results
                        continue
                    logger.debug(f"Downloading manifest data for {vessel.vessel_name}")
                    # Download the manifest
                    for _, record in result.iterrows():
                        product_id = record.iloc[1]
                        product_name = record.iloc[2]

                        manifest_content = None
                        manifest_dir = Path.cwd() / "metadata"
                        manifest_dir.mkdir(exist_ok=True)

                        for attempt in range(2):
                            try:
                                manifest_content = download_manifest(
                                        api_session,
                                        product_id,
                                        product_name,
                                        settings.CATALOGUE_URL
                                )
                                break
                            except Exception as e:
                                # Retry if the token is invalid
                                if "401" in str(e):
                                    access_token = authenticate(settings.AUTH_URL, settings.USERNAME, settings.PASSWORD)
                                    api_session.headers["Authorization"] = f"Bearer {access_token}"
                                else:
                                    # Retry if the connection is lost
                                    break

                        # Skip if the manifest is not found
                        if manifest_content is None:
                            continue

                        lat_str = f"{float(lat):0.5f}"
                        lon_str = f"{float(lon):0.5f}"

                        manifest_path = manifest_dir / f"{lat_str}_{lon_str}_MTD_MSIL1C.xml"

                        # Save the manifest
                        save_to_file(manifest_path, manifest_content)

                        band_locations = parse_manifest(manifest_path)

                        # Create the jp2 patches directory
                        jp2_patches_dir = Path.cwd() / "Assets" / "jp2_patches"
                        jp2_patches_dir.mkdir(exist_ok=True)

                        # Create the filename
                        filename = f"{product_id}"
                        filename = filename.replace(".SAFE", "")
                        
                        logger.debug(f"Downloading bands for {vessel.vessel_name}")
                        # Download bands
                        bands = download_bands(
                                api_session,
                                product_id,
                                product_name,
                                band_locations,
                                settings.CATALOGUE_URL,
                                jp2_patches_dir,
                                filename
                        )
                        logger.debug(f"Creating patches for {vessel.vessel_name}")
                        if not bands is None:
                            patch_names = create_cropped_patches(
                                    bands,
                                    (1024, 1024),
                                    jp2_patches_dir,
                                    filename,
                                    (1024, 1024)
                            )

                        composite_patches = Path.cwd() / "Assets" / "composite_patches"
                        composite_patches.mkdir(exist_ok=True)

                        logger.debug(f"Creating composite image for {vessel.vessel_name}")
                        # Create the composite image
                        # TODO fix this !!! some variables/functions are not declared 
                        for patch_name in patch_names:
                            rgb_path = generate_composite_image(
                                # TODO is this generate_composite_image from inference.py?
                                # The function returns a path to the composite image 
                                    jp2_patches_dir,
                                    composite_patches,
                                    filename
                            )
                            if rgb_path: 
                                # Run inference
                                logger.debug(f"Running inference for {vessel.vessel_name}")
                                # TODO the result of the ship detection inference isn't being used anywhere 
                                result_path = run_ship_detection(rgb_path, api_session)

                                # Save the pass
                                sat_pass = SatPass(
                                        satellite="Sentinel-2",
                                        timestamp=datetime.strptime(filename[:15], "%Y%m%dT%H%M%S"),
                                        latitude=lat,
                                        longitude=lon,
                                        image_url=rgb_path
                                )

                                # Assign pass to status
                                vessel.statuses[-1].passes.append(sat_pass)

                                session.add(sat_pass)
                                session.commit()