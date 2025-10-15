import requests
import json

BASE_URL = "http://127.0.0.1:5000/api/storage/v2/records"
HEADERS = {"data-partition-id": "tenant1"}

def test_get_records():
    # Example IDs to test with
    ids = [
        "osdu:doc:record-020",  # expected to exist
        "osdu:doc:record-021"   # likely missing
    ]

    # 1. Default: latest only, exclude deleted
    resp = requests.get(
        BASE_URL,
        headers=HEADERS,
        params={"ids": ",".join(ids)}
    )
    print("=== Latest only (default) ===")
    print("Status:", resp.status_code)
    print(json.dumps(resp.json(), indent=2))

    # 2. Include deleted
    resp = requests.get(
        BASE_URL,
        headers=HEADERS,
        params={"ids": ",".join(ids), "includeDeleted": "true"}
    )
    print("\n=== Include deleted ===")
    print("Status:", resp.status_code)
    print(json.dumps(resp.json(), indent=2))

    # 3. Explicit latest=false (all versions, if supported)
    resp = requests.get(
        BASE_URL,
        headers=HEADERS,
        params={"ids": ",".join(ids), "latest": "false"}
    )
    print("\n=== All versions (latest=false) ===")
    print("Status:", resp.status_code)
    print(json.dumps(resp.json(), indent=2))


if __name__ == "__main__":
    test_get_records()

