import requests
import json

SCHEMA_SERVICE_URL = "http://127.0.0.1:5001/api/schema-service/v1/schemas"
SCHEMA_ID = "osdu:wks:master-data--Wellbore:1.1.0"

def fetch_resolved_schema(schema_id):
    url = f"{SCHEMA_SERVICE_URL}/{schema_id}"
    params = {"resolve": "true"}
    resp = requests.get(url, params=params)
    print(f"Requesting: {resp.url}")
    print("Status Code:", resp.status_code)
    try:
        schema_json = resp.json()
        print(json.dumps(schema_json, indent=2))
    except Exception:
        print("Response was not valid JSON:")
        print(resp.text)

if __name__ == "__main__":
    fetch_resolved_schema(SCHEMA_ID)
