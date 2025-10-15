import requests

url = "http://localhost:5000/api/storage/v2/records/osdu:doc:record-002"
headers = {"data-partition-id": "12345"}
params = {"kind": "osdu:wks:work-product-component--WellLog:1.0.0"}

response = requests.get(url, headers=headers, params=params)
print("Status Code:", response.status_code)
print("Response JSON:", response.json())
