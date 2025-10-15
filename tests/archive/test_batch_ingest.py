import requests

url = "http://127.0.0.1:5000/api/storage/v2/records:batch"
headers = {"data-partition-id": "12345"}

payload = {
    "records": [
        {
            "id": "osdu:doc:record-004",
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
                "Name": "Batch Well Log 1",
                "WellID": "osdu:doc:well-2001",
                "LogType": "GammaRay",
                "StartDepth": 1000,
                "EndDepth": 1500,
                "DepthUnit": "m"
            }
        },
        {
            "id": "osdu:doc:record-005",
            "kind": "osdu:wks:master-data--Well:1.4.0",
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
                "Name": "Batch Well 2002",
                "WellID": "well-2002",
                "Country": "USA",
                "Operator": "Demo Operator Inc.",
                "SpudDate": "2021-01-01"
            }
        }
    ]
}

response = requests.post(url, headers=headers, json=payload)
print("Status Code:", response.status_code)
print("Response:", response.json())
