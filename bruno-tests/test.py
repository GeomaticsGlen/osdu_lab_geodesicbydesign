import requests

resp = requests.get(
    "http://127.0.0.1:5001/api/schema-service/v1/schema",
    headers={
        "Authorization": "Bearer dev-placeholder",
        "data-partition-id": "opendes"
    }
)
print(resp.status_code)
print(resp.json())