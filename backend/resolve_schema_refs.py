#backend/resolve_schema_refs.py
import os
import json
from datetime import datetime

# Root directory of your schema files
SCHEMA_ROOT = (
    r"E:\dataprocessing\osdu_github_repos\osdu-data-data-definitions"
    r"\SchemaRegistrationResources\shared-schemas\osdu"
)

# === MODE FLAG ===
# Set to True when running in dry-run mode (dummy run)
# Set to False when running in real deployment mode
DRY_RUN = False   # <<< CHANGE THIS FLAG


# --- Tracking sets for summary ---
RESOLVED_REFS = set()
UNRESOLVED_REFS = set()


def normalize_ref(ref_string):
    """
    Normalize $ref strings like:
    - 'osdu:wks:AbstractLegalTags:1.0.0'
    - 'wks:AbstractLegalTags:1.0.0'
    → 'wks:AbstractLegalTags:1.0.0'
    """
    parts = ref_string.split(":")
    if len(parts) == 4 and parts[0] == "osdu" and parts[1] == "wks":
        return f"wks:{parts[2]}:{parts[3]}"
    return ref_string


def parse_wks_ref(ref_string):
    """
    Parse 'wks:AbstractAccessControlList:1.0.0'
    → ('abstract', 'AbstractAccessControlList.1.0.0')
    """
    if not ref_string.startswith("wks:"):
        return None
    try:
        _, name, version = ref_string.split(":")
        schema_id = f"{name}.{version}"

        # Infer schema_type from name prefix
        if name.startswith("Abstract"):
            schema_type = "abstract"
        elif name.startswith("Reference"):
            schema_type = "reference-data"
        elif name.startswith("Well") or name.startswith("Trajectory"):
            schema_type = "master-data"
        else:
            schema_type = "other"

        return schema_type, schema_id
    except ValueError:
        return None


def load_schema(schema_type, schema_id):
    """
    Load schema JSON from disk given type and full ID like 'AbstractCommonResources.1.0.0'
    """
    filename = f"{schema_id}.json"
    path = os.path.join(SCHEMA_ROOT, schema_type, filename)
    if not os.path.exists(path):
        print(f"❌ Missing: {path}")
        return None
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def resolve_refs(schema, visited=None):
    """
    Recursively resolve $ref entries inside a schema dict.
    Tracks resolved and unresolved references.
    """
    if visited is None:
        visited = set()

    if isinstance(schema, dict):
        if "$ref" in schema:
            ref_value = normalize_ref(schema["$ref"])
            if ref_value in visited:
                return {}  # prevent circular refs
            visited.add(ref_value)

            parsed = parse_wks_ref(ref_value)
            if not parsed:
                print(f"⚠️ Unrecognized $ref format: {ref_value}")
                UNRESOLVED_REFS.add(ref_value)
                return schema

            schema_type, schema_id = parsed
            ref_schema = load_schema(schema_type, schema_id)
            if not ref_schema:
                print(f"⚠️ Could not resolve $ref: {ref_value}")
                UNRESOLVED_REFS.add(ref_value)
                return schema

            RESOLVED_REFS.add(ref_value)
            return resolve_refs(ref_schema, visited)

        return {k: resolve_refs(v, visited) for k, v in schema.items()}

    elif isinstance(schema, list):
        return [resolve_refs(item, visited) for item in schema]

    else:
        return schema


def fetch_and_resolve(kind):
    """
    Given a kind like 'wks:Well:1.4.0', resolve and return the full schema with $ref expanded.
    """
    parsed = parse_wks_ref(kind)
    if not parsed:
        raise ValueError(f"Invalid kind format: {kind}")

    schema_type, schema_id = parsed
    root_schema = load_schema(schema_type, schema_id)
    if not root_schema:
        raise FileNotFoundError(f"Schema not found: {schema_type}/{schema_id}")

    return resolve_refs(root_schema)


# === Top-level function for import ===
def resolve_schema_refs(schema_dict):
    """
    Public entry point: resolve all $ref in a given schema dict.
    This is the function imported by local_DeploySharedSchemas.py
    """
    return resolve_refs(schema_dict)


def write_summary_log():
    """
    Write a summary log file with resolved and unresolved references.
    File is timestamped and marked as DRYRUN or REALRUN.
    """
    mode = "DRYRUN" if DRY_RUN else "REALRUN"
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_filename = f"schema_ref_summary_{mode}_{timestamp}.txt"

    with open(log_filename, "w", encoding="utf-8") as log:
        log.write(f"Schema Reference Resolution Summary — {datetime.now()}\n")
        log.write(f"Mode: {mode}\n")
        log.write("=" * 60 + "\n\n")

        log.write("✅ Resolved References:\n")
        if RESOLVED_REFS:
            for ref in sorted(RESOLVED_REFS):
                log.write(f"  {ref}\n")
        else:
            log.write("  (none)\n")

        log.write("\n❌ Unresolved References:\n")
        if UNRESOLVED_REFS:
            for ref in sorted(UNRESOLVED_REFS):
                log.write(f"  {ref}\n")
        else:
            log.write("  (none)\n")

    print(f"\n📄 Summary log written to {log_filename}")


def main():
    # Example: resolve Well.1.4.0
    schema_type = "master-data"
    schema_id = "Well.1.4.0"

    root_schema = load_schema(schema_type, schema_id)
    if not root_schema:
        print(f"❌ Root schema not found: {schema_type}/{schema_id}")
        return

    resolved = resolve_schema_refs(root_schema)
    print(json.dumps(resolved, indent=2))

    # At the end, write summary log
    write_summary_log()


if __name__ == "__main__":
    main()
