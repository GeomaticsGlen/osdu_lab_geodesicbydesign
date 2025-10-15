import requests
import json

BASE_URL = "http://127.0.0.1:5000/api/storage/v2/records:delete"
HEADERS = {"data-partition-id": "tenant1"}

def main():
    # Generate the IDs from 1015 to 1020 inclusive
    ids = [f"osdu:master-data--Well:well-{i}" for i in range(1015, 1021)]

    payload = {"ids": ids}

    resp = requests.post(BASE_URL, headers=HEADERS, json=payload)
    print("Status:", resp.status_code)
    try:
        print(json.dumps(resp.json(), indent=2))
    except Exception:
        print("Raw response:", resp.text)

if __name__ == "__main__":
    main()
