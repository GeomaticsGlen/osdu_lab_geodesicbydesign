import json
import os
from services.schema_service import validate_record, resolve_schema
from app import create_app  # assuming your Flask app is defined in app.py


SEQ_FILE = r"E:\dataprocessing\osdu_github_repos\osdu-data-data-definitions\ReferenceValues\Manifests\reference-data\IngestionSequence.json"
ROOT_DIR = os.path.dirname(SEQ_FILE)
LOG_FILE = "manifest_validation_errors.log"

def normalize_path(file_name: str) -> str:
    prefix = "ReferenceValues/Manifests/reference-data/"
    if file_name.startswith(prefix):
        file_name = file_name[len(prefix):]
    return os.path.join(ROOT_DIR, file_name)

def load_payload(manifest_path: str):
    with open(manifest_path, "r", encoding="utf-8") as mf:
        manifest = json.load(mf)

    if isinstance(manifest, dict):
        if "records" in manifest:
            return manifest["records"]
        else:
            return [manifest]
    elif isinstance(manifest, list):
        return manifest
    else:
        raise ValueError(f"Unexpected manifest format in {manifest_path}")

def validate_manifest(file_path: str, records: list, manifest_key: str):
    errors = []
    for idx, record in enumerate(records, start=1):
        record_id = record.get("id", "<missing>")
        kind = record.get("kind", "<missing>")
        try:
            print(f"🔍 Resolving schema for kind: {kind}")
            schema = resolve_schema(kind)
            print(f"✅ Schema resolved for kind: {kind}")
            validate_record(record)
        except Exception as e:
            error_msg = f"Record {idx} (ID: {record_id}) in {manifest_key}: {e}"
            errors.append(error_msg)
            with open(LOG_FILE, "a", encoding="utf-8") as log:
                log.write(error_msg + "\n")
    return errors

def main():
    with open(SEQ_FILE, "r", encoding="utf-8") as f:
        sequence = json.load(f)

    total = len(sequence)
    total_errors = 0
    skipped_manifests = 0

    print(f"\n🔍 Preflight validation of {total} manifests...\n")

    for idx, entry in enumerate(sequence, start=1):
        manifest_path = normalize_path(entry["FileName"])
        manifest_key = entry["Key"]
        print(f"[{idx}/{total}] Checking {manifest_key} ({entry['kind']})")

        if not os.path.exists(manifest_path):
            print(f"❌ Missing file: {manifest_path}")
            total_errors += 1
            skipped_manifests += 1
            continue

        try:
            records = load_payload(manifest_path)
        except Exception as e:
            print(f"❌ Failed to load manifest: {e}")
            total_errors += 1
            skipped_manifests += 1
            continue

        if not records:
            print("⚠️ Empty payload.")
            continue

        errors = validate_manifest(manifest_path, records, manifest_key)
        if errors:
            print(f"❌ {len(errors)} validation error(s):")
            for err in errors:
                print("  - " + err)
            total_errors += len(errors)
            skipped_manifests += 1
        else:
            print(f"✅ All {len(records)} record(s) passed validation.")

    print("\n================ VALIDATION SUMMARY ================")
    print(f"Total manifests checked: {total}")
    print(f"Total validation errors: {total_errors}")
    print(f"Manifests skipped due to errors: {skipped_manifests}")
    print(f"Error log written to: {LOG_FILE}")
    print("====================================================")

if __name__ == "__main__":
    app = create_app()
    with app.app_context():
        main()

