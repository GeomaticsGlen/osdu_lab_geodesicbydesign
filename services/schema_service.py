# services/schema_service.py
#
# Provides schema registration, retrieval, and validation helpers.

import json
from jsonschema import validate, ValidationError
from flask import current_app
from db import get_conn
from backend.resolve_schema_refs import fetch_and_resolve as external_resolve

# Optional: Extract semantic version from kind string
def register_schema(schema: dict) -> str:
    schema_id = schema.get("id")
    kind = schema.get("kind")
    if not schema_id or not kind:
        raise ValueError("Schema must include both 'id' and 'kind' fields")

    # Extract status from top-level or schemaInfo
    status = schema.get("status") or schema.get("schemaInfo", {}).get("status") or "PUBLISHED"

    # Auto-normalize from kind string
    # Example: osdu:wks:reference-data--AddressType:1.0.0
    try:
        parts = kind.split(":")
        authority = parts[0] if len(parts) > 0 else "unknown"
        source = parts[1] if len(parts) > 1 else "unknown"
        entity_type_version = parts[2] if len(parts) > 2 else "generic:1.0.0"
        if ":" in entity_type_version:
            entity_type, version_str = entity_type_version.split(":")
        else:
            entity_type = entity_type_version
            version_str = "1.0.0"
        version_parts = version_str.split(".")
        version_major = int(version_parts[0]) if len(version_parts) > 0 else 1
        version_minor = int(version_parts[1]) if len(version_parts) > 1 else 0
        version_patch = int(version_parts[2]) if len(version_parts) > 2 else 0
        version = f"{version_major}.{version_minor}.{version_patch}"
    except Exception as e:
        current_app.logger.warning(f"⚠️ Failed to parse kind '{kind}': {e}")
        authority = "unknown"
        source = "unknown"
        entity_type = "generic"
        version_major, version_minor, version_patch = 1, 0, 0
        version = "1.0.0"

    class_name = schema.get("class")

    conn = get_conn()
    cur = conn.cursor()
    try:
        cur.execute("""
            INSERT INTO schema_registry (
                id, kind, status, version, schema,
                created_time, modify_time,
                authority, source, entity_type,
                version_major, version_minor, version_patch,
                class
            )
            VALUES (%s, %s, %s, %s, %s,
                    now(), now(),
                    %s, %s, %s,
                    %s, %s, %s,
                    %s)
            ON CONFLICT (id) DO UPDATE
            SET kind = EXCLUDED.kind,
                status = EXCLUDED.status,
                version = EXCLUDED.version,
                schema = EXCLUDED.schema,
                modify_time = now(),
                authority = EXCLUDED.authority,
                source = EXCLUDED.source,
                entity_type = EXCLUDED.entity_type,
                version_major = EXCLUDED.version_major,
                version_minor = EXCLUDED.version_minor,
                version_patch = EXCLUDED.version_patch,
                class = EXCLUDED.class
        """, (
            schema_id, kind, status, version, json.dumps(schema),
            authority, source, entity_type,
            version_major, version_minor, version_patch,
            class_name
        ))
        conn.commit()
        return schema_id
    except Exception as e:
        conn.rollback()
        current_app.logger.exception(f"❌ SQL error while registering schema {schema_id}: {e}")
        raise
    finally:
        cur.close()


# Retrieve schema by kind
def get_schema_by_kind(kind: str):
    conn = get_conn()
    cur = conn.cursor()
    try:
        cur.execute("SELECT schema FROM schema_registry WHERE kind = %s", (kind,))
        row = cur.fetchone()
        if not row:
            return None
        definition = row[0]
        if isinstance(definition, str):
            definition = json.loads(definition)
        return definition
    finally:
        cur.close()

# === Retrieve Schema by ID ===
# This function queries the schema_registry table for a schema definition using its unique 'id'.
# It returns the parsed JSON schema if found, or None if no matching record exists.
# The schema is stored as a JSON string in the database, so we deserialize it before returning.
# This is useful for direct lookups when the full schema ID is known (e.g., osdu:wks:reference-data--WellLogPassType:1.0.0).
# Ensures cursor is closed after execution to avoid connection leaks.
def get_schema_by_id(schema_id: str):
    conn = get_conn()
    cur = conn.cursor()
    try:
        cur.execute("SELECT schema FROM schema_registry WHERE id = %s", (schema_id,))
        row = cur.fetchone()
        if not row:
            return None
        definition = row[0]
        if isinstance(definition, str):
            definition = json.loads(definition)
        return definition
    finally:
        cur.close()
# Resolve schema by kind (local or external)
def resolve_schema(kind: str) -> dict:
    schema = get_schema_by_kind(kind)
    if schema:
        return schema
    return external_resolve(kind)

# Validate full record against resolved schema
def validate_record(record: dict):
    record_id = record.get("id", "<missing>")
    current_app.logger.info(f"🔍 Validating record: {record_id}")

    required_fields = ["id", "kind", "legal", "acl", "data"]
    for field in required_fields:
        if field not in record:
            current_app.logger.error(f"❌ Record {record_id} missing required field: {field}")
            raise ValueError(f"Missing required field: {field}")

    schema = resolve_schema(record["kind"])
    try:
        validate(instance=record["data"], schema=schema)
        current_app.logger.info(f"✅ Record {record_id} passed schema validation")
    except ValidationError as ve:
        current_app.logger.error(f"❌ Record {record_id} failed schema validation: {ve.message}")
        raise ValueError(f"Schema validation failed: {ve.message}")

# Validate raw data payload against schema
def validate_data_against_schema(kind: str, data: dict):
    current_app.logger.info(f"🔍 Validating data against schema for kind: {kind}")
    schema = resolve_schema(kind)
    try:
        validate(instance=data, schema=schema)
        current_app.logger.info(f"✅ Data passed schema validation for kind: {kind}")
    except ValidationError as ve:
        current_app.logger.error(f"❌ Schema validation failed for kind {kind}: {ve.message}")
        raise ValueError(ve.message)
