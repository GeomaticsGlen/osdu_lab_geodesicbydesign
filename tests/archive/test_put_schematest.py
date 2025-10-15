import requests
import json

# Target URL for the Storage service PUT /records route
url = "http://127.0.0.1:5000/api/storage/v2/records"

# Headers (OSDU services often expect a data-partition-id header)
headers = {
    "Content-Type": "application/json",
    "data-partition-id": "opendes"   # adjust to your partition if needed
}

# Sample payload: list of records
# This record uses a Well schema kind; adjust to match a schema you have loaded
payload = [
    {
        "id": "osdu:master-data--Well:12345",
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
            "Name": "Test Well",
            "Operator": "Demo Operator",
            "Country": "UK",
            "SpudDate": "2020-01-01"
        }
    }
]

# Send the PUT request
response = requests.put(url, headers=headers, data=json.dumps(payload))

print("Status Code:", response.status_code)

try:
    print("Response JSON:", json.dumps(response.json(), indent=2))
except Exception:
    print("Response Text:", response.text)
