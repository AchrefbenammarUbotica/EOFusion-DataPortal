from beyond.dates import Date, timedelta
from geopy.distance import geodesic
from src.logger import log_function_call_debug , get_logger
from beyond.io.tle import Tle
import numpy as np
import math

logger = get_logger(__name__)

@log_function_call_debug(logger)
def get_ground_track(sat, t_start, t_stop, t_sample, obs_lat, obs_lon):
    """
    Calculate distance of passes over a given window of time

    Args:
        sat (beyond.TLE): Satellite TLE object
        t_start (datetime.datetime): Start time
        t_stop (datetime.timedelta): Hours to step through
        t_sample (datetime.timedelta): Time between samples
        obs_lat (float): Observer latitude
        obs_lon (float): Observer longitude

    Returns:
        list: List of tuples containing lat/lon coordinates
        list: List of tuples containing lat/lon coordinates, distance from observer, date, and satellite
    """
    ground_track_coords = []
    points = []

    # Calculate the orbit path from the TLE
    orbit = sat.orbit()

    t_start = Date.strptime(t_start, '%Y-%m-%d %H:%M:%S')

    # Step through the orbit and calculate distance from the observer
    for point in orbit.ephemeris(start=t_start, stop=timedelta(hours=t_stop), step=timedelta(seconds=t_sample)):
        point.frame = "ITRF"
        point.form = "spherical"
        # Convert to Lat/Lon
        lon, lat = np.degrees(point[1:3])
        # Satellite footprint
        ground_track_coords.append((lat, lon))

        # Calculate distance from observer
        if (obs_lat):
            g = geodesic((obs_lat, obs_lon), (lat, lon)).kilometers
            points.append([(lat, lon), g, point.date.strftime('%Y-%m-%d %H:%M:%S'), sat.name])

            points.sort(key=lambda x: x[1])

    return ground_track_coords, points

@log_function_call_debug(logger)
def get_closest_pass(lat, lon, timedate, tles):
    """
    Get the closest pass to a given lat/lon

    Args:
        lat (float): Latitude
        lon (float): Longitude
        timedate (datetime.datetime): Time to check
        tles (list): List of TLEs

    Returns:
        list: List of closest passes
    """

    overall_closest = []

    # For each TLE, calculate the closest pass
    for tle in tles:
        sat = Tle(tle.line1 + "\n" + tle.line2)

        # Get the ground track 
        _, points = get_ground_track(sat, timedate, 24, 60, lat, lon)

        if len(overall_closest) < 10:
            overall_closest.append(points[0])
        else:
            overall_closest.sort(key=lambda x: x[1])
            if points[0][1] < overall_closest[-1][1]:
                overall_closest[-1] = points[0]

    return overall_closest

@log_function_call_debug(logger)
def get_orbit(tle):
    """
    Get the orbit of a satellite

    Args:
        tle (TLE): Satellite TLE object

    Returns:
        list: List of tuples containing lat/lon coordinates
    """
    orbit = tle.orbit()

    orbit_coords = []

    for point in orbit.ephemeris(start=Date.now(), stop=timedelta(days=1), step=timedelta(minutes=120)):
        point.frame = "ITRF"
        point.form = "spherical"
        lon, lat = np.degrees(point[1:3])
        orbit_coords.append((lat, lon))

    return orbit_coords

@log_function_call_debug(logger)
def add_distance_to_gps(lat, lon, distance, bearing):
    """
    Add distance to a GPS coordinate

    Args:
        lat (float): Latitude
        lon (float): Longitude
        distance (float): Distance in km
        bearing (float): Bearing in degrees

    Returns:
        tuple: Tuple containing new lat/lon coordinates
    """

    R = 6378.1 # Radius of the Earth

    brng = math.radians(bearing) # Bearing is degrees converted to radians.
    rlat = math.radians(lat) # Current lat point converted to radians
    rlon = math.radians(lon) # Current lon point converted to radians
    rdist = distance / R # Distance in km converted to radians

    new_lat = math.asin(math.sin(rlat) * math.cos(rdist) + math.cos(rlat) * math.sin(rdist) * math.cos(brng))
    new_lon = rlon + math.atan2(math.sin(brng) * math.sin(rdist) * math.cos(rlat), math.cos(rdist) - math.sin(rlat) * math.sin(new_lat))

    new_lat = math.degrees(new_lat)
    new_lon = math.degrees(new_lon)

    return new_lat, new_lon