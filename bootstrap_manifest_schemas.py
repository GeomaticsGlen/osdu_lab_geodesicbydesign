import os
import json
import requests
from datetime import datetime

# === Configuration ===
REPO_ROOT = r"E:\dataprocessing\osdu_github_repos\osdu-data-data-definitions"
SCHEMA_BASE = os.path.join(REPO_ROOT, "SchemaRegistrationResources")
SEQUENCE_FILE = os.path.join(SCHEMA_BASE, "shared-schemas", "osdu", "load_sequence.1.0.0.json")
SCHEMA_API = "http://localhost:5000/api/schema-service/v1/schema"
HEADERS = {"Content-Type": "application/json"}
DRY_RUN = False  # Set to True to simulate ingestion without POSTing
RESOLVED_DIR = os.path.join(os.getcwd(), "resolved_manifest_schemas")

# === Helpers ===
def load_schema_file(path):
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)

def extract_kind(schema_dict, fname):
    kind = schema_dict.get("kind")
    if not kind:
        kind = schema_dict.get("schemaInfo", {}).get("schemaIdentity", {}).get("id")
    if not kind:
        print(f"❌ Missing 'kind' in {fname}")
    return kind

def register_schema(kind, schema_body):
    payload = {
        "id": kind,
        "kind": kind,
        "status": "published",
        "schema": schema_body
    }
    if DRY_RUN:
        return "🧪 Dry-run: skipped POST"
    try:
        resp = requests.post(SCHEMA_API, headers=HEADERS, json=payload)
        if resp.status_code == 201:
            return "✅ Registered"
        elif resp.status_code == 409:
            return "⚠️ Already exists"
        else:
            return f"❌ Failed ({resp.status_code}): {resp.text}"
    except Exception as e:
        return f"❌ Exception: {e}"

def write_summary_log(results, total, success, skipped, failed):
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    mode = "DRYRUN" if DRY_RUN else "REALRUN"
    log_file = f"manifest_schema_ingestion_summary_{mode}_{timestamp}.txt"

    with open(log_file, "w", encoding="utf-8") as log:
        log.write(f"Manifest Schema Ingestion Summary — {datetime.now()}\n")
        log.write(f"Mode: {mode}\n")
        log.write("=" * 60 + "\n\n")
        for line in results:
            log.write(line + "\n")
        log.write("\n================ INGESTION SUMMARY ================\n")
        log.write(f"Total schemas processed: {total}\n")
        log.write(f"✅ Registered: {success}\n")
        log.write(f"⚠️ Already existed: {skipped}\n")
        log.write(f"❌ Failed: {failed}\n")
        log.write("===================================================\n")

    print(f"\n📄 Summary log written to {log_file}")

def dump_resolved_batch(batch, batch_index):
    os.makedirs(RESOLVED_DIR, exist_ok=True)
    for item in batch:
        kind = item["kind"].replace(":", "_").replace("--", "_")
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"{kind}_{timestamp}.json"
        path = os.path.join(RESOLVED_DIR, filename)
        with open(path, "w", encoding="utf-8") as f:
            json.dump(item["schema"], f, indent=2)
        print(f"🧪 Dumped: {filename}")

def write_resolution_status_log(status_log):
    os.makedirs(RESOLVED_DIR, exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"resolved_manifest_status_log_{timestamp}.json"
    path = os.path.join(RESOLVED_DIR, filename)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(status_log, f, indent=2)
    print(f"📄 Resolution status log written to {filename}")

def load_sequence():
    try:
        with open(SEQUENCE_FILE, "r", encoding="utf-8") as f:
            return json.load(f)  # list of {kind, relativePath}
    except Exception as e:
        print(f"❌ Failed to load sequence file: {e}")
        return []

# === Main ===
def main():
    sequence = load_sequence()
    total = len(sequence)
    success, skipped, failed = 0, 0, 0
    results = []
    dryrun_batch = []
    batch_index = 1
    resolution_status = []

    print(f"\n📦 Ingesting {total} schemas based on load_sequence.1.0.0.json...\n")

    for idx, entry in enumerate(sequence, start=1):
        kind = entry.get("kind")
        rel_path = entry.get("relativePath")
        if not kind or not rel_path:
            results.append(f"[{idx}/{total}] {kind or 'UNKNOWN'} → ❌ Missing kind or relativePath")
            resolution_status.append({
                "filename": rel_path,
                "kind": kind,
                "status": "unresolved",
                "reason": "Missing kind or relativePath"
            })
            failed += 1
            continue

        abs_path = os.path.join(SCHEMA_BASE, rel_path.replace("/", os.sep))
        if not os.path.isfile(abs_path):
            results.append(f"[{idx}/{total}] {kind} → ❌ File not found: {rel_path}")
            resolution_status.append({
                "filename": rel_path,
                "kind": kind,
                "status": "unresolved",
                "reason": "File not found"
            })
            failed += 1
            continue

        try:
            schema = load_schema_file(abs_path)

            # Structural validation
            missing_fields = []
            if not schema.get("schema"):
                missing_fields.append("schema")

            if missing_fields:
                results.append(f"[{idx}/{total}] {rel_path} → ❌ Missing fields: {', '.join(missing_fields)}")
                resolution_status.append({
                    "filename": rel_path,
                    "kind": kind,
                    "status": "unresolved",
                    "reason": f"Missing fields: {', '.join(missing_fields)}"
                })
                failed += 1
                continue

            result = register_schema(kind, schema)
            results.append(f"[{idx}/{total}] {rel_path} → {result}")

            if DRY_RUN and result.startswith("🧪"):
                dryrun_batch.append({ "filename": rel_path, "kind": kind, "schema": schema })
                resolution_status.append({
                    "filename": rel_path,
                    "kind": kind,
                    "status": "resolved",
                    "reason": "Schema structurally valid"
                })
                if len(dryrun_batch) == 10:
                    dump_resolved_batch(dryrun_batch, batch_index)
                    batch_index += 1
                    dryrun_batch = []

            else:
                resolution_status.append({
                    "filename": rel_path,
                    "kind": kind,
                    "status": "unresolved",
                    "reason": "Schema not batched or failed registration"
                })

            if result.startswith("✅"):
                success += 1
            elif result.startswith("⚠️") or result.startswith("🧪"):
                skipped += 1
            else:
                failed += 1

        except Exception as e:
            results.append(f"[{idx}/{total}] {rel_path} → ❌ Error: {e}")
            resolution_status.append({
                "filename": rel_path,
                "kind": kind,
                "status": "unresolved",
                "reason": f"Exception: {e}"
            })
            failed += 1

    # Dump remaining batch
    if DRY_RUN and dryrun_batch:
        dump_resolved_batch(dryrun_batch, batch_index)

    # Write resolution status log
    if DRY_RUN:
        write_resolution_status_log(resolution_status)

    for line in results:
        print(line)

    print("\n================ INGESTION SUMMARY ================")
    print(f"Total schemas processed: {total}")
    print(f"✅ Registered: {success}")
    print(f"⚠️ Already existed / Dry-run: {skipped}")
    print(f"❌ Failed: {failed}")
    print("===================================================")

    write_summary_log(results, total, success, skipped, failed)

if __name__ == "__main__":
    main()
