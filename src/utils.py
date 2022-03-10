import json
import requests


def fetch_data(start_date, end_date, start=0, rows=100):
    payload = {
        "grantFromDate": start_date,
        "grantToDate": end_date,
        "start": start,
        "rows": rows,
        "largeTextSearchFlag": "N"
    }
    base_url = "https://developer.uspto.gov/ibd-api/v1/application/grants?"
    response = requests.get(base_url, params=payload)
    response.raise_for_status()
    return response


def persist_data_to_json_file(filename, data):
    with open(filename, 'w') as f:
        json.dump(data, f)
