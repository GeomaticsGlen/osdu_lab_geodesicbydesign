import json
import logging
from datetime import datetime
from typing import List, Dict
from jsonschema import validate, ValidationError
from db import get_conn
from backend.resolve_schema_refs import fetch_and_resolve as external_resolve

logger = logging.getLogger(__name__)

# -------------------- Registration --------------------

def register_schema(schema: dict) -> str:
    schema_id = schema.get("id")
    kind = schema.get("kind")
    if not schema_id or not kind:
        raise ValueError("Schema must include both 'id' and 'kind' fields")

    status = schema.get("status") or schema.get("schemaInfo", {}).get("status") or "PUBLISHED"

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
        logger.warning(f"⚠️ Failed to parse kind '{kind}': {e}")
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
        logger.exception(f"❌ SQL error while registering schema {schema_id}: {e}")
        raise
    finally:
        cur.close()

# -------------------- Retrieval --------------------

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

def resolve_schema(kind: str) -> dict:
    schema = get_schema_by_kind(kind)
    if schema:
        return schema
    return external_resolve(kind)

# -------------------- Validation --------------------

def validate_record(record: dict):
    record_id = record.get("id", "<missing>")
    logger.info(f"🔍 Validating record: {record_id}")

    required_fields = ["id", "kind", "legal", "acl", "data"]
    for field in required_fields:
        if field not in record:
            logger.error(f"❌ Record {record_id} missing required field: {field}")
            raise ValueError(f"Missing required field: {field}")

    schema = resolve_schema(record["kind"])
    try:
        validate(instance=record["data"], schema=schema)
        logger.info(f"✅ Record {record_id} passed schema validation")
    except ValidationError as ve:
        logger.error(f"❌ Record {record_id} failed schema validation: {ve.message}")
        raise ValueError(f"Schema validation failed: {ve.message}")

def validate_data_against_schema(kind: str, data: dict):
    logger.info(f"🔍 Validating data against schema for kind: {kind}")
    schema = resolve_schema(kind)
    try:
        validate(instance=data, schema=schema)
        logger.info(f"✅ Data passed schema validation for kind: {kind}")
    except ValidationError as ve:
        logger.error(f"❌ Schema validation failed for kind {kind}: {ve.message}")
        raise ValueError(ve.message)

# -------------------- Flattening --------------------

def get_registered_field_types(kind: str) -> List[Dict[str, str]]:
    """
    Extracts all field names and types from the schema_registry for a given kind.
    Used by the /schema/fields route to flatten schema definitions for frontend display.
    """
    conn = get_conn()
    with conn.cursor() as cur:
        cur.execute("""
            SELECT schema FROM schema_registry
            WHERE kind = %s
            LIMIT 1
        """, (kind,))
        row = cur.fetchone()
        if not row:
            return []

        schema_def = row[0]
        if isinstance(schema_def, str):
            schema_def = json.loads(schema_def)

        data_fields = schema_def.get("properties", {}).get("data", {}).get("properties", {})

        return [
            {"field": field, "type": field_def.get("type", "unknown")}
            for field, field_def in sorted(data_fields.items())
        ]

def get_flattened_data_fields(kind: str) -> List[Dict[str, str]]:
    """
    Recursively flattens all fields from schema_registry.schema->'schema'->'properties'->'data'.
    Handles nested objects, arrays, relationships, and $ref targets.
    """
    conn = get_conn()
    resolved_cache = {}

    def fetch_schema(kind: str) -> Dict:
        if kind in resolved_cache:
            return resolved_cache[kind]
        with conn.cursor() as cur:
            cur.execute("""
                SELECT schema->'schema' FROM schema_registry
                WHERE kind = %s
                LIMIT 1
            """, (kind,))
            row = cur.fetchone()
            if not row:
                return {}
            schema_def = row[0]
            resolved_cache[kind] = schema_def
            return schema_def

    flattened = []

    def flatten_properties(properties: Dict, path: str = ""):
        for field, attrs in properties.items():
            full_path = f"{path}{field}"
            flat = {"field": full_path}
            for key, val in attrs.items():
                flat[key] = str(val)
            flattened.append(flat)

            # Recurse into nested object properties
            if attrs.get("type") == "object" and "properties" in attrs:
                flatten_properties(attrs["properties"], path=f"{full_path}.")
            # Recurse into array items
            elif attrs.get("type") == "array" and "items" in attrs:
                items = attrs["items"]
                if isinstance(items, dict):
                    if "properties" in items:
                        flatten_properties(items["properties"], path=f"{full_path}[].")
                    elif "allOf" in items:
                        for block in items["allOf"]:
                            if "properties" in block:
                                flatten_properties(block["properties"], path=f"{full_path}[].")

            # 🔗 Relationship resolution
            if "x-osdu-relationship" in attrs:
                try:
                    rel = attrs["x-osdu-relationship"][0]
                    ref_kind = f"osdu:wks:reference-data--{rel['EntityType']}:1.0.0"
                    ref_schema = fetch_schema(ref_kind)
                    ref_props = ref_schema.get("properties", {}).get("data", {}).get("properties", {})
                    for subfield, subattrs in ref_props.items():
                        sub = {"field": f"{full_path}.{subfield}"}
                        for k, v in subattrs.items():
                            sub[k] = str(v)
                        flattened.append(sub)
                except Exception as e:
                    logger.warning(f"⚠️ Failed to resolve relationship for {full_path}: {e}")

            # 📦 $ref resolution
            if "$ref" in attrs:
                try:
                    ref_id = attrs["$ref"]
                    if ref_id.startswith("osdu:wks:"):
                        ref_kind = ref_id
                        ref_schema = fetch_schema(ref_kind)
                        ref_props = ref_schema.get("properties", {})
                        flatten_properties(ref_props, path=f"{full_path}.")
                except Exception as e:
                    logger.warning(f"⚠️ Failed to resolve $ref for {full_path}: {e}")

    # Load main schema
    main_schema = fetch_schema(kind)

    # Handle data.allOf blocks
    data_allof = main_schema.get("properties", {}).get("data", {}).get("allOf", [])
    for block in data_allof:
        if "properties" in block:
            flatten_properties(block["properties"])

    # Handle direct data.properties
    direct_props = main_schema.get("properties", {}).get("data", {}).get("properties", {})
    if direct_props:
        flatten_properties(direct_props)

    return flattened