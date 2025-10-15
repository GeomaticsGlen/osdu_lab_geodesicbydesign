import requests

# Target the record you ingested earlier
url = "http://127.0.0.1:5000/api/storage/v2/records/osdu:doc:record-001"

# Match the tenant header you used when ingesting
headers = {
    "data-partition-id": "12345"
}

response = requests.delete(url, headers=headers)

print("Status Code:", response.status_code)

try:
    print("Response JSON:", response.json())
except Exception:
    print("Response Text:", response.text)
