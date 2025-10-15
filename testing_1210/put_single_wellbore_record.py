import requests
import json

# Endpoint
url = "http://localhost:5000/api/storage/v2/records"

# Headers
headers = {
    "Content-Type": "application/json",
    "data-partition-id": "opendes"
}

# Load record from file
with open("borehole_example-999-bore-1.json", "r", encoding="utf-8") as f:
    record = json.load(f)

# Send PUT request
response = requests.put(url, headers=headers, data=json.dumps(record))

# Output result
print("Status:", response.status_code)
print(json.dumps(response.json(), indent=2))
