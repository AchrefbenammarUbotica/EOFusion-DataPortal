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



