import requests

url = "http://127.0.0.1:5000/api/storage/v2/records:batch"
headers = {"data-partition-id": "tenant1"}

payload = {
    "records": [
        {
            "id": "osdu:doc:record-010",
            "kind": "osdu:wks:work-product-component--WellLog:1.0.0",
            "acl": {
                "owners": ["data.default.owners@tenant1.osdu"],
                "viewers": ["data.default.viewers@tenant1.osdu"]
            },
            "legal": {
                "legaltags": ["tenant1-legaltag"],
                "otherRelevantDataCountries": ["US"],
                "status": "compliant"
            },
            "data": {
                "Name": "Batch Well Log 4",
                "WellID": "osdu:doc:well-2007",
                "LogType": "GammaRay",
                "StartDepth": 500,
                "EndDepth": 1200,
                "DepthUnit": "m"
            }
        },
        {
            # Purposely broken (missing 'kind')
            "id": "osdu:doc:record-011",
            "acl": {
                "owners": ["data.default.owners@tenant1.osdu"],
                "viewers": ["data.default.viewers@tenant1.osdu"]
            },
            "legal": {
                "legaltags": ["tenant1-legaltag"],
                "otherRelevantDataCountries": ["US"],
                "status": "compliant"
            },
            "data": {
                "Name": "Broken Record",
                "WellID": "osdu:doc:well-2008"
            }
        }
    ]
}

response = requests.post(url, headers=headers, json=payload)
print("Status Code:", response.status_code)
print("Response:", response.json())
