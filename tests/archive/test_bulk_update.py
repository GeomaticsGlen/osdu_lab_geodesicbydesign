import requests
import json

url = "http://127.0.0.1:5000/api/storage/v2/records:patch"
headers = {"data-partition-id": "tenant1"}

payload = {
    "records": [
        {
            "id": "osdu:master-data--Well:well-1002",
            "data": {
                "WellStatus": "Plugged and Abandoned"
            }
        },
        {
            "id": "osdu:master-data--Well:well-9999",
            "data": {
                "WellStatus": "Active"
            }
        }
    ]
}

resp = requests.post(url, headers=headers, json=payload)
print("Status:", resp.status_code)
print(json.dumps(resp.json(), indent=2))
