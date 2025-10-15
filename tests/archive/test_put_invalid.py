import requests
import json

# Target URL for the Storage service PUT /records route
url = "http://127.0.0.1:5000/api/storage/v2/records"

headers = {
    "Content-Type": "application/json",
    "data-partition-id": "opendes"
}

# Payload missing the required field "SpudDate"
payload = [
    {
        "id": "osdu:master-data--Well:99999",
        "kind": "osdu:wks:master-data--Well:1.4.0",
        "legal": {
            "legaltags": ["opendes:UK"],
            "otherRelevantDataCountries": ["GB"],
            "status": "compliant"
        },
        "acl": {
            "owners": ["data.default.owners@opendes.org"],
            "viewers": ["data.default.viewers@opendes.org"]
        },
        "data": {
            "Name": "Invalid Well",
            "Operator": "Demo Operator",
            "Country": "UK"
            # "SpudDate" intentionally omitted
        }
    }
]

response = requests.put(url, headers=headers, data=json.dumps(payload))

print("Status Code:", response.status_code)

try:
    print("Response JSON:", json.dumps(response.json(), indent=2))
except Exception:
    print("Response Text:", response.text)
