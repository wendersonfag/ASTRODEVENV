# TODO import libraries
import requests
import json

from requests.auth import HTTPBasicAuth
from datetime import datetime

# TODO set airflow configs
AIRFLOW_API_URL = "http://localhost:8080/api/v1/dags"
USERNAME = "admin"
PASSWORD = "admin"

# TODO request payload
payload = {
    "dag_id": 'tf-bitcoin'
}


# TODO make the POST request to trigger the DAG
response = requests.get(
    AIRFLOW_API_URL,
    auth=HTTPBasicAuth(USERNAME, PASSWORD),
    headers={"Content-Type": "application/json"},
    data=json.dumps(payload)
)

# TODO check the response
if response.status_code == 200:
    print(f"Successfully triggered DAG with run ID: {response.text}")
else:
    print(f"Failed to trigger DAG. Status Code: {response.status_code}")
    print(f"Response: {response.text}")