import pytest
from unittest.mock import patch, MagicMock
import requests
from datetime import datetime
import json
from src.logger import get_logger, log_function_call_debug
from pathlib import Path
import pandas as pd
import os
from src.services.inference import (
    authenticate,
    query_catalogue,
    download_manifest,
    parse_manifest,
    download_bands,
    generate_composite_image,
    create_cropped_patches,
    save_to_file,
    run_ship_detection
)

logger = get_logger(__name__)

# Test authenticate function
@patch('requests.post')
def test_authenticate(mock_post):
    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.text = json.dumps({'access_token': 'test_token'})
    mock_post.return_value = mock_response

    token = authenticate('http://example.com', 'user', 'pass')
    assert token == 'test_token'
    mock_post.assert_called_once_with('http://example.com', data={
        "client_id": "cdse-public",
        "grant_type": "password",
        "username": 'user',
        "password": 'pass'
    }, verify=True, allow_redirects=False)

@patch('requests.get')
def test_query_catalogue(mock_get):
    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.json.return_value = {"value": [{"id": 1, "name": "product1"}]}
    mock_get.return_value = mock_response
    
    df = query_catalogue(
        'http://example.com', 'collection', 'product_type', 'aoi', 10,  datetime(2025, 1, 1),datetime(2025, 12, 31)
    )
    
    assert isinstance(df, pd.DataFrame)
    assert df.shape[0] == 1
    mock_get.assert_called_once()


@patch('requests.Session.get')  
def test_download_manifest(mock_get):
    mock_response = MagicMock()
    mock_response.status_code = 200  # Simulate a successful response
    mock_response.content = b'fake_manifest_content'  # Fake content of the manifest
    mock_response.text = "Fake manifest file"  # Simulate response text
    mock_get.return_value = mock_response  # Set the mock response as return value

    # Mock session
    mock_session = MagicMock()
    mock_session.get = mock_get  # Ensure session.get() uses the mock

    # Call the function under test
    manifest = download_manifest(mock_session, 'product_id', 'product_name', 'http://example.com')

    # Assertions
    assert manifest == b'fake_manifest_content'  # Check if returned content is correct
    mock_get.assert_called()  # Ensure get() was actually called



@patch('requests.get')
def test_download_bands(mock_get):
    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.content = b'band_content'
    mock_get.return_value = mock_response
    
    bands = download_bands(MagicMock(), 'product_id', 'product_name', ['band1', 'band2'], 'http://example.com', Path('.'), 'output_name')
    assert len(bands) == 2

def test_generate_composite_image():
    # Assuming the files exist, in a real test this would need to mock rasterio
    image_path = generate_composite_image('jp2_patches', 'output', 'output_name')
    assert isinstance(image_path, Path)
    assert image_path.exists()

def test_create_cropped_patches():
    # Mock the rasterio.open method and ensure that the patches are created
    with patch('rasterio.open') as mock_open:
        mock_band = MagicMock()
        mock_band.width = 1000
        mock_band.height = 1000
        mock_open.return_value.__enter__.return_value = mock_band
        
        patches = create_cropped_patches(['band1', 'band2'], output_dir=Path('./'), output_name='test_output')
        assert len(patches) > 0

def test_save_to_file():
    data = b'file_data'
    with patch('builtins.open', mock_open()) as mock_file:
        save_to_file(data, 'test_file.txt')
        mock_file.assert_called_once_with('test_file.txt', 'wb')
        mock_file.return_value.write.assert_called_once_with(data)

@patch('ultralytics.YOLO')
def test_run_ship_detection(mock_model):
    mock_model.return_value = MagicMock()
    mock_result = MagicMock()
    mock_result.orig_shape = (1000, 1000)
    mock_result.obb = [MagicMock(cls=MagicMock(item=MagicMock(return_value=1)))]
    mock_model.return_value.__call__.return_value = [mock_result]
    
    result_path = run_ship_detection('test_image.jpg')
    assert isinstance(result_path, str)
    assert result_path.endswith('.jpg')
