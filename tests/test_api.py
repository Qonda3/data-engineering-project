#!/usr/bin/env python3
"""Test the Usage API"""

import requests
import json
from requests.auth import HTTPBasicAuth

BASE_URL = "http://localhost:18089"
API_USER = "admin"
API_PASS = "password"

def test_health():
    """Test health check endpoint"""
    print("Testing health check...")
    try:
        response = requests.get(f"{BASE_URL}/health", timeout=5)
        print(f"Status: {response.status_code}")
        print(f"Response: {response.json()}")
        print()
    except Exception as e:
        print(f"Error: {e}")
        print()

def test_valid_query():
    """Test valid usage query"""
    print("Testing valid usage query...")
    params = {
        'msisdn': '2712345678',
        'start_time': '20240101000000',
        'end_time': '20240101235959'
    }
    
    try:
        response = requests.get(
            f"{BASE_URL}/data_usage",
            params=params,
            auth=HTTPBasicAuth(API_USER, API_PASS),
            timeout=5
        )
        print(f"Status: {response.status_code}")
        print(f"Response: {json.dumps(response.json(), indent=2)}")
        print()
    except Exception as e:
        print(f"Error: {e}")
        print()

def test_missing_auth():
    """Test missing authentication"""
    print("Testing missing authentication...")
    params = {
        'msisdn': '2712345678',
        'start_time': '20240101000000',
        'end_time': '20240101235959'
    }
    
    try:
        response = requests.get(
            f"{BASE_URL}/data_usage",
            params=params,
            timeout=5
        )
        print(f"Status: {response.status_code}")
        print(f"Response: {response.text}")
        print()
    except Exception as e:
        print(f"Error: {e}")
        print()

def test_missing_params():
    """Test missing query parameters"""
    print("Testing missing query parameters...")
    
    try:
        response = requests.get(
            f"{BASE_URL}/data_usage",
            auth=HTTPBasicAuth(API_USER, API_PASS),
            timeout=5
        )
        print(f"Status: {response.status_code}")
        print(f"Response: {response.json()}")
        print()
    except Exception as e:
        print(f"Error: {e}")
        print()

def test_invalid_datetime():
    """Test invalid datetime format"""
    print("Testing invalid datetime format...")
    params = {
        'msisdn': '2712345678',
        'start_time': '2024-01-01',  # Wrong format
        'end_time': '20240101235959'
    }
    
    try:
        response = requests.get(
            f"{BASE_URL}/data_usage",
            params=params,
            auth=HTTPBasicAuth(API_USER, API_PASS),
            timeout=5
        )
        print(f"Status: {response.status_code}")
        print(f"Response: {response.json()}")
        print()
    except Exception as e:
        print(f"Error: {e}")
        print()

if __name__ == '__main__':
    print("=" * 60)
    print("Usage API Test Suite")
    print("=" * 60)
    print()
    
    test_health()
    test_valid_query()
    test_missing_auth()
    test_missing_params()
    test_invalid_datetime()
    
    print("All tests completed!")