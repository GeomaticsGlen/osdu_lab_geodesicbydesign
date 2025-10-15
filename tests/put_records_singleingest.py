# test_put.py
import requests
import json

url = "http://localhost:5000/api/storage/v2/records"
headers = {
    "Authorization": "Bearer your-auth-token",
    "data-partition-id": "osdu",
    "Content-Type": "application/json"
}

payload = [{
    "id": "osdu:well--test-well-001",
    "kind": "osdu:wks:master-data--Well:1.4.0",
    "acl": {
        "owners": ["data.default.owner@osdu"],
        "viewers": ["data.default.viewer@osdu"]
    },
    "legal": {
        "legaltags": ["osdu-default-legal"],
        "otherRelevantDataCountries": ["US"],
        "status": "compliant"
    },
    "data": {
        "Name": "Test Well Alpha",
        "WellID": "TW-001",
        "Operator": "Test Energy Corp",
        "SpudDate": "2020-01-15T00:00:00Z",
        "SurfaceLocation": {
            "Latitude": 29.76,
            "Longitude": -95.36,
            "CRS": "EPSG:4326"
        },
        "Elevation": {"Value": 120.5, "Unit": "m"},
        "Datum": "MSL",
        "Country": "USA",
        "State": "Texas",
        "County": "Harris",
        "Field": "Test Field",
        "Block": "Block A",
        "Basin": "Gulf Coast",
        "Play": "Play X",
        "WellStatus": "Active",
        "WellPurpose": "Production",
        "WellFluid": "Oil",
        "WellDirection": "Vertical",
        "WellHeadElevation": {"Value": 125.0, "Unit": "m"},
        "WellHeadElevationDatum": "MSL",
        "CreatedBy": "stuart.g@osdu.org",
        "CreationDate": "2025-10-15T11:00:00Z"
    }
}]

response = requests.put(url, headers=headers, data=json.dumps(payload))
print(f"Status Code: {response.status_code}")
print(json.dumps(response.json(), indent=2))
