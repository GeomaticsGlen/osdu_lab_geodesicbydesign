import requests

# Target the record you want to patch
url = "http://127.0.0.1:5000/api/storage/v2/records/osdu:doc:record-002"

# Match the tenant header you used when ingesting
headers = {
    "data-partition-id": "12345"
}

# Fields to update (partial update)
payload = {
    "data": {
        "Description": "Updated description via PATCH",
        "LogType": "Density",
        "EndDepth": 1650.0
    }
}

response = requests.patch(url, headers=headers, json=payload)

print("Status Code:", response.status_code)

try:
    print("Response JSON:", response.json())
except Exception:
    print("Response Text:", response.text)
