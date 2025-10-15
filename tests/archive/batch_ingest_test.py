import requests
import json

# Storage ingestion endpoint
URL = "http://localhost:8080/api/storage/v2/records"

# Headers (replace dummy-token with a real one if needed)
HEADERS = {
    "Content-Type": "application/json",
    "Authorization": "Bearer dummy-token"
}

# Valid record
valid_record = {
    "id": "osdu:master-data--Wellbore:1234567890",
    "kind": "osdu:wks:master-data--Wellbore:1.1.0",
    "acl": {
        "owners": ["owner@example.com"],
        "viewers": ["viewer@example.com"]
    },
    "ancestry": {
        "parents": []
    },
    "legal": {
        "legaltags": ["opendes:US"],
        "otherRelevantDataCountries": ["US"],
        "status": "compliant"
    },
    "data": {
        "NameAliases": [
            {
                "AliasName": "Test Wellbore",
                "AliasNameTypeID": "osdu:reference-data--AliasNameType:regulatory:1"
            }
        ],
        "SpatialLocation": {
            "type": "Point",
            "coordinates": [0.0, 0.0],
            "crs": {
                "type": "name",
                "properties": {
                    "name": "EPSG:4326"
                }
            }
        }
    }
}

# Invalid record (missing required 'owners' in acl)
invalid_record = {
    "id": "osdu:master-data--Wellbore:badcase",
    "kind": "osdu:wks:master-data--Wellbore:1.1.0",
    "acl": {
        "viewers": ["viewer@example.com"]
    },
    "ancestry": {
        "parents": []
    },
    "legal": {
        "legaltags": ["opendes:US"],
        "otherRelevantDataCountries": ["US"],
        "status": "compliant"
    },
    "data": {
        "NameAliases": [
            {
                "AliasName": "Broken Wellbore",
                "AliasNameTypeID": "osdu:reference-data--AliasNameType:regulatory:1"
            }
        ]
    }
}

# Batch payload
payload = {
    "records": [valid_record, invalid_record]
}

def main():
    try:
        response = requests.post(URL, headers=HEADERS, data=json.dumps(payload))
        print("Status Code:", response.status_code)

        # Try to parse JSON, fallback to raw text
        try:
            resp_json = response.json()
            pretty = json.dumps(resp_json, indent=2)
        except Exception:
            pretty = response.text

        print("Response:\n", pretty)

        # Save to file
        with open("batch_ingestion_result.json", "w", encoding="utf-8") as f:
            f.write(pretty)

        print("\n[✔] Response written to batch_ingestion_result.json")

    except Exception as e:
        print("Error during request:", e)

if __name__ == "__main__":
    main()
