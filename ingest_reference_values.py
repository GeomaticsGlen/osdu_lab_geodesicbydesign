import json
import os
import requests
import sys
from services.schema_service import validate_record
from app import create_app  # Import the Flask app factory

# === Flask App Context ===
app = create_app()

# === Configuration ===
BASE = "http://localhost:5000"
HEADERS = {
    "Content-Type": "application/json",
    "data-partition-id": "osdu",
    "Authorization": "Bearer dev-placeholder"
}

SEQ_FILE = r"E:\dataprocessing\osdu_github_repos\osdu-data-data-definitions\ReferenceValues\Manifests\reference-data\IngestionSequence.json"
ROOT_DIR = os.path.dirname(SEQ_FILE)
DRY_RUN = False  # Set to True to simulate ingestion without POSTing
LOG_FILE = "ingestion_summary.log"

REQUIRED_FIELDS = ["id", "kind", "acl", "legal", "data"]

DEFAULT_ACL = {
    "viewers": ["data.default.viewers@opendes.example.com"],
    "owners": ["data.default.owners@opendes.example.com"]
}

DEFAULT_LEGAL = {
    "legaltags": ["opendes-public-usa-dataset-1"],
    "otherRelevantDataCountries": ["US"]
}

def normalize_path(file_name: str) -> str:
    prefix = "ReferenceValues/Manifests/reference-data/"
    if file_name.startswith(prefix):
        file_name = file_name[len(prefix):]
    return os.path.join(ROOT_DIR, file_name)

def load_payload(manifest_path: str):
    with open(manifest_path, "r", encoding="utf-8") as mf:
        manifest = json.load(mf)

    # Case 1: Wrapped in "records"
    if isinstance(manifest, dict) and "records" in manifest:
        return manifest

    # Case 2: Wrapped in "ReferenceData"
    if isinstance(manifest, dict) and "ReferenceData" in manifest:
        return {"records": manifest["ReferenceData"]}

    # Case 3: Pure array of records
    if isinstance(manifest, list):
        return {"records": manifest}

    # Case 4: Single record dict
    if isinstance(manifest, dict):
        return {"records": [manifest]}

    raise ValueError(f"Unexpected manifest format in {manifest_path}")

def preflight_validate(records, kind, key):
    for record in records:
        # Inject synthetic ID if missing
        if "id" not in record:
            record["id"] = f"osdu:reference-data--{kind}:{key}"

        # Auto-fill missing ACL and LEGAL
        record.setdefault("acl", DEFAULT_ACL)
        record.setdefault("legal", DEFAULT_LEGAL)

        # Check required fields
        for field in REQUIRED_FIELDS:
            if field not in record:
                raise ValueError(f"Missing required field: {field}")

        # Schema validation
        validate_record(record)

def main():
    with open(SEQ_FILE, "r", encoding="utf-8") as f:
        sequence = json.load(f)

    results = []
    total = len(sequence)

    for idx, entry in enumerate(sequence, start=1):
        manifest_path = normalize_path(entry["FileName"])
        print(f"\n[{idx}/{total}] Ingesting {entry['Key']} ({entry['kind']})")

        if not os.path.exists(manifest_path):
            results.append((entry["Key"], entry["kind"], "MISSING FILE"))
            print(f"⚠️ Missing file: {manifest_path}")
            continue

        try:
            payload = load_payload(manifest_path)
        except Exception as e:
            results.append((entry["Key"], entry["kind"], f"BAD FORMAT ({e})"))
            print(f"⚠️ Bad format in {manifest_path}: {e}")
            continue

        records = payload.get("records", [])
        if not records:
            results.append((entry["Key"], entry["kind"], "EMPTY PAYLOAD"))
            print("⚠️ No records found in payload.")
            continue

        # === Preflight validation ===
        try:
            preflight_validate(records, entry["kind"], entry["Key"])
        except Exception as e:
            results.append((entry["Key"], entry["kind"], f"VALIDATION FAIL ({e})"))
            print(f"❌ Validation failed: {e}")
            continue

        if DRY_RUN:
            results.append((entry["Key"], entry["kind"], "DRY RUN"))
            print("🧪 Dry run — skipping POST")
            continue

        print(f"Attempting to ingest {len(records)} record(s)...")
        resp = requests.post(f"{BASE}/api/storage/v2/records:batch",
                             headers=HEADERS, json=payload)

        try:
            response_json = resp.json()
        except Exception:
            response_json = {}

        if resp.status_code == 201:
            errors = response_json.get("recordErrors", [])
            if errors:
                status = f"PARTIAL FAIL ({len(errors)} errors)"
                print("⚠️ Partial ingestion errors:")
                for err in errors:
                    print(f" - ID: {err.get('id')} | Code: {err.get('code')} | Reason: {err.get('reason')}")
            else:
                status = "SUCCESS"
        else:
            status = f"FAIL ({resp.status_code})"
            print("❌ Ingestion failed:")
            print(resp.text)

        results.append((entry["Key"], entry["kind"], status))

    # === Summary report ===
    print("\n================ SUMMARY REPORT ================")
    success_count = sum(1 for _, _, s in results if s == "SUCCESS")
    fail_count = sum(1 for _, _, s in results if s not in ["SUCCESS", "DRY RUN"])
    dry_count = sum(1 for _, _, s in results if s == "DRY RUN")
    print(f"Total manifests processed: {len(results)}")
    print(f"✅ Success: {success_count}")
    print(f"🧪 Dry run: {dry_count}")
    print(f"❌ Failures: {fail_count}")

    if fail_count > 0:
        print("\nFailed entries:")
        for key, kind, status in results:
            if status not in ["SUCCESS", "DRY RUN"]:
                print(f" - {key} ({kind}) -> {status}")

    # === Write to log file ===
    with open(LOG_FILE, "w", encoding="utf-8") as log:
        for key, kind, status in results:
            log.write(f"{key},{kind},{status}\n")

if __name__ == "__main__":
    with app.app_context():
        main()
