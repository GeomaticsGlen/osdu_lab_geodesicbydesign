import requests
import json

STORAGE_URL = "http://127.0.0.1:5000/api/storage/v2/records"
HEADERS = {
    "Content-Type": "application/json",
    "data-partition-id": "opendes"
}

def put_records(payload, label):
    print(f"\n--- {label} ---")
    resp = requests.put(STORAGE_URL, headers=HEADERS, data=json.dumps(payload))
    print("Status Code:", resp.status_code)
    try:
        print("Response JSON:", json.dumps(resp.json(), indent=2))
    except Exception:
        print("Response Text:", resp.text)

WELL RECORD (minimal, schema-derived):
{
  "id": "osdu:master-data--Well:WELL-1001",
  "kind": "osdu:wks:master-data--Well:1.4.0",
  "acl": {
    "owners": [
      "data.default.owners@opendes.org"
    ],
    "viewers": [
      "data.default.viewers@opendes.org"
    ]
  },
  "legal": {
    "legaltags": [
      "opendes:UK"
    ],
    "otherRelevantDataCountries": [
      "GB"
    ],
    "status": "compliant"
  },
  "data": {
    "kind": "string",
    "acl": "value",
    "legal": "value"
  }
}

WELLBORE RECORD (minimal, schema-derived):
{
  "id": "osdu:master-data--Wellbore:WB-2001",
  "kind": "osdu:wks:master-data--Wellbore:1.1.0",
  "acl": {
    "owners": [
      "data.default.owners@opendes.org"
    ],
    "viewers": [
      "data.default.viewers@opendes.org"
    ]
  },
  "legal": {
    "legaltags": [
      "opendes:UK"
    ],
    "otherRelevantDataCountries": [
      "GB"
    ],
    "status": "compliant"
  },
  "data": {
    "kind": "string",
    "acl": "value",
    "legal": "value"
  }
}


if __name__ == "__main__":
    put_records(well_payload, "Insert Parent Well")
    put_records(wellbore_valid, "Insert Valid Wellbore")
    put_records(wellbore_invalid, "Insert Invalid Wellbore (missing Name)")
