from src.logger import get_logger, log_function_call_debug
from rasterio.windows import Window
import xml.etree.ElementTree as ET
import matplotlib.pyplot as plt
from datetime import datetime
from ultralytics import YOLO
from pathlib import Path
import pandas as pd
import numpy as np
import rasterio
import requests
import json
import os 

model_path = os.path.join("assets" , "s2_ship_detection_yolov8_obb.pt")
model = YOLO(model_path)
logger = get_logger(__name__)

# Tested successfully 
@log_function_call_debug(logger=logger)
def authenticate(auth_url, username, password):
    """
    Authenticate to the API

    Args: auth_url: str: URL to authenticate to
        username: str: username
        password: str: password
    Returns:
        str: access token
    Raises:
        Exception: if the authentication fails
    """
    data = {
            "client_id": "cdse-public",
            "grant_type": "password",
            "username": username,
            "password": password
    }

    response = requests.post(auth_url, data=data, verify=True, allow_redirects=False)

    if response.status_code == 200:
        return json.loads(response.text).get("access_token")

    else:
        raise Exception("Error Authenticating\nError {}: {}".format(response.status_code, response.text))

# Tested successfully 
@log_function_call_debug(logger=logger)
def query_catalogue(
    catalogue_odata_url : str , 
    collection_name : str ,
    product_type : str, 
    aoi : str, 
    max_cloud_cover : int , 
    search_period_start : datetime, 
    search_period_end : datetime):
    """
    Query the Copernicus Data Space Ecosystem (CSDE) OData catalogue for specific EO products

    Args:
        catalogue_odata_url: str: URL to the OData catalogue
        collection_name: str: name of the collection
        product_type: str: product type
        aoi: str: area of interest
        max_cloud_cover: int: maximum cloud cover
        search_period_start: str: start date of the search period
        search_period_end: str: end date of the search period
    Returns:
        pd.DataFrame: dataframe with the search results
    Raises:
        Exception: if the query fails
    """
    logger.debug(f"test/{catalogue_odata_url}")
    search_period_start = search_period_start.strftime("%Y-%m-%d")
    search_period_end = search_period_end.strftime("%Y-%m-%d")

    query = (
            f"{catalogue_odata_url}/Products?$filter="
            f"Collection/Name eq '{collection_name}' and "
            f"Attributes/OData.CSC.StringAttribute/any(att:att/Name eq 'productType' "
            f"and att/OData.CSC.StringAttribute/Value eq '{product_type}') and "
            f"OData.CSC.Intersects(area=geography'SRID=4326;{aoi}') and "
            f"ContentDate/Start gt {search_period_start} and "
            f"ContentDate/Start lt {search_period_end} and "
            f"Attributes/OData.CSC.DoubleAttribute/any(att:att/Name eq 'cloudCover' "
            f"and att/OData.CSC.DoubleAttribute/Value le {max_cloud_cover})"
    )

    response = requests.get(query)

    if response.status_code != 200:
        raise Exception("Error Querying Catalogue\nError {}: {}".format(response.status_code, response.text))

    return pd.DataFrame.from_dict(response.json().get("value", []))
# Tested successfully
@log_function_call_debug(logger=logger)
def download_manifest(session, product_id, product_name, catalogue_url):
    """
    Download the manifest of a product from the catalogue and save it to disk in the output directory as a .xml file

    Args:
        session: requests.session: session with the access token
        product_id: str: product ID
        product_name: str: product name
        catalogue_url: str: URL to the catalogue
    Returns:
        bytes: manifest content
    Raises:
        Exception: if the download fails or the manifest is not found
    """

    url = f"{catalogue_url}/Products('{product_id}')/Nodes({product_name})/Nodes(MTD_MSIL1C.xml)/$value"
    response = session.get(url, allow_redirects=False)

    while response.status_code in (301, 302, 303, 307):
        url = response.headers["Location"]
        response = session.get(url, allow_redirects=False)

    if response.status_code != 200:
        raise Exception("Error Downloading Manifest\nError {}: {}".format(response.status_code, response.text))

    return response.content
@log_function_call_debug(logger=logger)
def parse_manifest(manifest_path):
    """
    Parse the manifest of a product

    Args:
        manifest_path: str: path to the manifest
    Returns:
        list: list of bands
    """

    tree = ET.parse(manifest_path)
    root = tree.getroot()

    try:
        bands = root[0][0][12][0][0]
    except IndexError as e :
        logger.error("Error Parsing Manifest: Bands not found : {e}")
        return None

    return [f"{bands[i].text}" for i in range(0,3)]

@log_function_call_debug(logger=logger)
def download_bands(session, product_id, product_name, band_locations, catalogue_url, output_dir, output_name):
    """
    Download the bands of a product and save them to disk in the output directory

    Args:
        session: requests.session: session with the access token
        product_id: str: product ID
        product_name: str: product name
        band_locations: list: list of band locations
        catalogue_url: str: URL to the catalogue
        output_dir: str: output directory
        output_name: str: output name
    Returns:
        list: list of band paths
    """

    bands = []
    for band_file in band_locations:
        band_parts = band_file.split("/")

        if len(band_parts) < 4:
            continue

        url = (
                f"{catalogue_url}/Products({product_id})/"
                f"Nodes({product_name})/Nodes({band_parts[0]})/"
                f"Nodes({band_parts[1]})/Nodes({band_parts[2]})/"
                f"Nodes({band_parts[3]})/$value"
        )

        response = session.get(url, allow_redirects=False)
        while response.status_code in (301, 302, 303, 307):
            url = response.headers["Location"]
            response = session.get(url, allow_redirects=False)

        if response.status_code == 200:
            file = session.get(url, verify=False, allow_redirects=True)
            tmp = band_parts[3].split("_")[-1]
            band_name = f"{output_name}_{tmp}"
            outfile = output_dir / band_name

            outfile.write_bytes(file.content)
            bands.append(str(outfile))

        else:
            logger.debug(f"Error Downloading Band {band_parts[3]}\nError {response.status_code}: {response.text}")

    return bands
@log_function_call_debug(logger=logger)
def generate_composite_image(jp2_patches_dir, output_dir, output_name):
    """
    Generate a composite image from the bands

    Args:
        jp2_patches_dir: str: path to the JP2 patches directory
        output_dir: str: output directory
        output_name: str: output name
    Returns:
        str: path to the composite image
    """

    red = None
    green = None
    blue = None

    # Load the bands
    blue_path = Path(jp2_patches_dir) / f"{output_name}_B02.jp2"
    green_path = Path(jp2_patches_dir) / f"{output_name}_B03.jp2"
    red_path = Path(jp2_patches_dir) / f"{output_name}_B04.jp2"

    if blue_path.exists():
        with rasterio.open(blue_path, driver="JP2OpenJPEG") as blue_band:
            blue = blue_band.read(1)
    else:
        logger.debug("Error: Blue band not found")

    if green_path.exists():
        with rasterio.open(green_path, driver="JP2OpenJPEG") as green_band:
            green = green_band.read(1)
    else:
        logger.debug("Error: Green band not found")

    if red_path.exists():
        with rasterio.open(red_path, driver="JP2OpenJPEG") as red_band:
            red = red_band.read(1)
    else:
        logger.debug("Error: Red band not found")


    if red is None or green is None or blue is None:
        logger.debug("Error: Bands not found")
        return []

    # Normalize the bands
    gain = 2
    red_n = np.clip(red * gain / 10000, 0, 1)
    green_n = np.clip(green * gain / 10000, 0, 1)
    blue_n = np.clip(blue * gain / 10000, 0, 1)

    # Create the composite image
    rgb_composite = np.dstack((red_n, green_n, blue_n))

    # Save the composite image
    output_image_path_rgb = Path(output_dir) / f"{output_name}_RGB.jpg"
    plt.imshow(output_image_path_rgb, rgb_composite)
    plt.close()
    return output_image_path_rgb

@log_function_call_debug(logger=logger)
def create_cropped_patches(bands, patch_size=(100,100), output_dir=None, output_name=None, step_size=(100,100)):
    """
    Create cropped patches from the bands

    Args:
        bands: list: list of band paths
        patch_size: tuple: size of the patch
        output_dir: str: output directory
        output_name: str: output name
        step_size: tuple: step size
    Returns:
        list: list of patch names
    Raises:
        Exception: if the band is not found or the patch size is larger than the band size
    """

    # Get the patch size and step size
    x_size, y_size = patch_size
    vertical_step, horizontal_step = step_size
    patch_names = []

    # Load the bands
    Path(output_dir).mkdir(parents=True, exist_ok=True)

    # Create the patches
    for n, band_file in enumerate(bands, start=2):
        try:
            # Open the band
            with rasterio.open(band_file, driver="JP2OpenJPEG") as full_band:
                # Get the band size
                full_width, full_height = full_band.width, full_band.height

                # Check if the patch size is larger than the band size
                if x_size > full_width or y_size > full_height:
                    logger.debug(f"Error: Patch size is larger than the band size")
                    continue

                # Create the patches
                for y_off in range(0, full_height, vertical_step):
                    for x_off in range(0, full_width, horizontal_step):
                        window = Window(x_off, y_off, x_size, y_size)
                        transform = full_band.window_transform(window)
                        profile = full_band.profile
                        profile.update({
                            "height": y_size,
                            "width": x_size,
                            "transform": transform
                        })

                        patch_band_name = f"{output_name}_patch_y{y_off}_x{x_off}_B0{n}.jp2"
                        patch_file_name = Path(output_dir) / patch_band_name

                        patch_name = patch_file_name.split("_band")[0]
                        patch_names.append(patch_name)

                        with rasterio.open(patch_file_name, "w", **profile) as patch_band:
                            patch_band.write(full_band.read(window=window))

        except (rasterio.errors.RasterioIOError, FileNotFoundError) as e:
            logger.debug(f"Error: Band {n} not found: {e}")
            continue

    return patch_names

@log_function_call_debug(logger=logger)
def save_to_file(data, filename):
    """
    Save data to a file

    Args:
        data: any: data to save
        filename: str: filename
    """
    with open(filename, "wb") as f:
        f.write(data)

@log_function_call_debug(logger=logger)
def run_ship_detection(image_path):
    """
    Run ship detection on an image

    Args:
        image_path: str: path to the image
    Returns:
        str: path to the results
    """

    os.makedirs("./assets/results", exist_ok=True)

    labels_results_dir = Path.cwd() / "assets" / "results" / "labels"
    inference_results_dir = Path.cwd() / "assets" / "results" / "inference"

    os.makedirs(labels_results_dir, exist_ok=True)
    os.makedirs(inference_results_dir, exist_ok=True)

    results = model(image_path)

    result = results[0]

    file_name = image_path.name

    result_path = os.path.join(inference_results_dir, file_name)
    results.save(result_path)

    label_file = file_name.replace(".jpg", ".txt").replace(".jpeg", ".txt").replace(".png", ".txt")
    label_file_path = os.path.join(labels_results_dir, label_file)

    width, height = result.orig_shape
    with open(label_file_path, "w") as f:
        for box in result.obb:
            class_id = int(box.cls.item())

            points = box.xyxyxyxy.cpu().numpy().flatten()

            normalized_points = [
                points[i] / width if i % 2 == 0 else points[i] / height for i in range(8)
            ]

            f.write(f"{class_id} {' '.join(map(str, normalized_points))}\n")

    return result_path