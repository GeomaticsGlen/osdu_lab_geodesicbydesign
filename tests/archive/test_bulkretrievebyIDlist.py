import requests
import json

url = "http://127.0.0.1:5000/api/storage/v2/records:retrieve"
headers = {"data-partition-id": "tenant1"}

payload = {
    "records": [
        "osdu:master-data--Well:well-1002",
        "osdu:master-data--Well:well-1015",
        "osdu:master-data--Well:well-9999"  # non-existent
    ]
}

resp = requests.post(url, headers=headers, json=payload, params={"includeDeleted": "true"})
print("Status:", resp.status_code)
print(json.dumps(resp.json(), indent=2))
