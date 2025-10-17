import json
import logging
from datetime import datetime
from typing import List, Dict
from fastapi import HTTPException
from db import get_conn
import json
from datetime import datetime
from db import get_conn
from services.schema_service import validate_data_against_schema
from services.schema_service import validate_record, validate_data_against_schema

logger = logging.getLogger(__name__)

# -------------------- Ingestion --------------------

def ingest_records(records: List[Dict]) -> Dict:
    conn = get_conn()
    ingested_ids, record_errors = [], []

    for record in records:
        record_id = record.get("id", "<missing>")
        cur = None
        try:
            validate_record(record)
            now = datetime.utcnow()
            cur = conn.cursor()
            cur.execute("SELECT version, data FROM records WHERE id = %s", (record["id"],))
            existing = cur.fetchone()

            if existing:
                current_version, existing_data = existing
                if isinstance(existing_data, str):
                    existing_data = json.loads(existing_data)
                if isinstance(existing_data, dict) and existing_data.get("osdu_deleted"):
                    existing_data.pop("osdu_deleted", None)
                    existing_data.pop("osdu_deleted_at", None)

                new_version = current_version + 1
                cur.execute("""
                    UPDATE records
                    SET kind = %s, legal = %s, acl = %s, data = %s,
                        version = %s, modify_user = %s, modify_time = %s
                    WHERE id = %s
                """, (
                    record["kind"],
                    json.dumps(record["legal"]),
                    json.dumps(record["acl"]),
                    json.dumps(record["data"]),
                    new_version,
                    "system",
                    now,
                    record["id"]
                ))
            else:
                cur.execute("""
                    INSERT INTO records (
                        id, kind, legal, acl, data, version,
                        create_user, create_time, modify_user, modify_time
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """, (
                    record["id"],
                    record["kind"],
                    json.dumps(record["legal"]),
                    json.dumps(record["acl"]),
                    json.dumps(record["data"]),
                    1,
                    "system",
                    now,
                    "system",
                    now
                ))

            conn.commit()
            ingested_ids.append(record["id"])

        except Exception as e:
            if conn:
                conn.rollback()
            logger.exception(f"Failed to ingest record {record_id}")
            record_errors.append({
                "id": record_id,
                "code": "DB_ERROR",
                "reason": str(e)
            })
        finally:
            if cur:
                cur.close()

    if not ingested_ids:
        raise HTTPException(status_code=400, detail={
            "error": "NO_RECORDS_COMMITTED",
            "reason": "All records failed validation or DB insert",
            "recordErrors": record_errors
        })

    return {
        "recordCount": len(ingested_ids),
        "recordIds": ingested_ids,
        "recordErrors": record_errors
    }

# -------------------- Retrieval --------------------

def get_records_by_ids(record_ids: List[str], include_deleted: bool = False) -> Dict:
    conn = get_conn()
    cur = conn.cursor()
    try:
        cur.execute("""
            SELECT id, kind, legal, acl, data, version,
                   create_user, create_time, modify_user, modify_time
            FROM records
            WHERE id = ANY(%s)
        """, (record_ids,))
        rows = cur.fetchall()

        found_records = []
        missing_ids = set(record_ids)

        for row in rows:
            rec_id, kind, legal, acl, data, version, create_user, create_time, modify_user, modify_time = row
            legal = json.loads(legal) if isinstance(legal, str) else legal
            acl = json.loads(acl) if isinstance(acl, str) else acl
            data = json.loads(data) if isinstance(data, str) else data

            if data.get("osdu_deleted") and not include_deleted:
                continue

            record = {
                "id": rec_id,
                "kind": kind,
                "acl": acl,
                "legal": legal,
                "data": data,
                "version": version,
                "createUser": create_user,
                "createTime": create_time.isoformat() if create_time else None,
                "modifyUser": modify_user,
                "modifyTime": modify_time.isoformat() if modify_time else None
            }
            found_records.append(record)
            missing_ids.discard(rec_id)

        return {
            "records": found_records,
            "missingRecordIds": list(missing_ids)
        }

    except Exception as e:
        logger.exception("Unhandled exception in get_records_by_ids")
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        cur.close()
# ------------------------------------------------------------------------------
# Service: patch_record
#
# Handles partial updates to a record. Merges incoming fields into the existing
# record, validates against schema, and updates the DB with a new version.
# ------------------------------------------------------------------------------

def patch_record(record_id, payload):
    conn = get_conn()
    cur = conn.cursor()
    try:
        # Fetch existing record
        cur.execute("""
            SELECT kind, legal, acl, data, version, osdu_deleted
            FROM records
            WHERE id = %s
        """, (record_id,))
        row = cur.fetchone()

        if not row:
            return ({"error": "Record not found"}), 404

        kind, legal, acl, data, version, osdu_deleted = row

        # Ensure JSON types
        if isinstance(legal, str):
            legal = json.loads(legal)
        if isinstance(acl, str):
            acl = json.loads(acl)
        if isinstance(data, str):
            data = json.loads(data)

        if osdu_deleted:
            return ({
                "error": "ALREADY_DELETED",
                "reason": "Cannot patch a deleted record"
            }), 400

        # Merge updates
        if "kind" in payload:
            kind = payload["kind"]
        if "legal" in payload:
            legal.update(payload["legal"])
        if "acl" in payload:
            acl.update(payload["acl"])
        if "data" in payload:
            data.update(payload["data"])

        # Schema validation
        try:
            validate_data_against_schema(kind, data)
        except ValueError as ve:
            return ({"error": "SCHEMA_VALIDATION_ERROR", "reason": str(ve)}), 400
        except Exception as e:
            return ({"error": "SCHEMA_SERVICE_ERROR", "reason": str(e)}), 500

        # Save back
        now = datetime.utcnow()
        new_version = version + 1
        cur.execute("""
            UPDATE records
            SET kind = %s,
                legal = %s,
                acl = %s,
                data = %s,
                version = %s,
                modify_user = %s,
                modify_time = %s
            WHERE id = %s
        """, (
            kind,
            json.dumps(legal),
            json.dumps(acl),
            json.dumps(data),
            new_version,
            "system",
            now,
            record_id
        ))
        conn.commit()

        return ({
            "id": record_id,
            "status": "patched",
            "updated_fields": list(payload.keys())
        }), 200

    except Exception as e:
        conn.rollback()
        logger.exception(f"Unhandled exception in patch_record for {record_id}")
        return ({"error": "Internal server error", "details": str(e)}), 500
    finally:
        cur.close()
def ingest_records_batch(records: List[Dict]) -> Dict:
    """
    Handles ingestion of multiple records in one request.
    Validates required fields, checks schema compliance,
    and inserts or updates records with versioning.
    """
    conn = get_conn()
    record_ids, record_errors = [], []

    for record in records:
        record_id = record.get("id", "<missing>")
        cur = None
        try:
            validate_record(record)

            now = datetime.utcnow()
            cur = conn.cursor()
            cur.execute("SELECT version FROM records WHERE id = %s", (record["id"],))
            existing = cur.fetchone()

            if existing:
                new_version = existing[0] + 1
                cur.execute("""
                    UPDATE records
                    SET kind = %s,
                        legal = %s,
                        acl = %s,
                        data = %s,
                        version = %s,
                        modify_user = %s,
                        modify_time = %s
                    WHERE id = %s
                """, (
                    record["kind"],
                    json.dumps(record["legal"]),
                    json.dumps(record["acl"]),
                    json.dumps(record["data"]),
                    new_version,
                    "system",
                    now,
                    record["id"]
                ))
            else:
                cur.execute("""
                    INSERT INTO records (
                        id, kind, legal, acl, data, version,
                        create_user, create_time, modify_user, modify_time
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """, (
                    record["id"],
                    record["kind"],
                    json.dumps(record["legal"]),
                    json.dumps(record["acl"]),
                    json.dumps(record["data"]),
                    1,
                    "system",
                    now,
                    "system",
                    now
                ))

            conn.commit()
            record_ids.append(record["id"])

        except Exception as e:
            if conn:
                conn.rollback()
            logger.exception(f"Failed to ingest record {record_id}")
            record_errors.append({
                "id": record_id,
                "code": "DB_ERROR",
                "reason": str(e)
            })
        finally:
            if cur:
                cur.close()

    if not record_ids:
        logger.warning("❌ No records were committed to the database.")
        raise HTTPException(status_code=400, detail={
            "error": "NO_RECORDS_COMMITTED",
            "reason": "All records failed validation or DB insert",
            "recordErrors": record_errors
        })

    return {
        "recordCount": len(record_ids),
        "recordIds": record_ids,
        "recordErrors": record_errors
    }
# ------------------------------------------------------------------------------
# Service: delete_records_bulk
#
# Handles soft-deletion of multiple records. Updates each record to set
# osdu_deleted=true and osdu_deleted_at timestamp. Returns structured response
# with successes and per-record errors.
# ------------------------------------------------------------------------------

def delete_records_bulk(ids):
    conn = get_conn()
    record_ids, record_errors = [], []

    for rid in ids:
        cur = None
        try:
            cur = conn.cursor()
            cur.execute("SELECT osdu_deleted FROM records WHERE id = %s", (rid,))
            row = cur.fetchone()

            if not row:
                record_errors.append({
                    "id": rid,
                    "code": "NOT_FOUND",
                    "reason": "Record not found"
                })
                continue

            already_deleted = row[0]
            now = datetime.utcnow()

            if already_deleted:
                record_errors.append({
                    "id": rid,
                    "code": "ALREADY_DELETED",
                    "reason": "Record already marked deleted"
                })
            else:
                cur.execute("""
                    UPDATE records
                    SET osdu_deleted = TRUE,
                        osdu_deleted_at = %s,
                        modify_user = %s,
                        modify_time = %s
                    WHERE id = %s
                """, (now, "system", now, rid))
                conn.commit()
                record_ids.append(rid)

        except Exception as e:
            conn.rollback()
            logger.exception(f"Failed to delete record {rid}")
            record_errors.append({
                "id": rid,
                "code": "DB_ERROR",
                "reason": str(e)
            })
        finally:
            if cur:
                cur.close()

    return ({
        "recordCount": len(record_ids),
        "recordIds": record_ids,
        "recordErrors": record_errors
    }), 200

def retrieve_records(ids, include_deleted=False, latest_only=True):
    conn = get_conn()
    cur = conn.cursor()
    try:
        # For now, latest_only and version history are the same query
        cur.execute("""
            SELECT id, kind, legal, acl, data, version,
                   create_user, create_time, modify_user, modify_time, osdu_deleted
            FROM records
            WHERE id = ANY(%s)
        """, (ids,))
        rows = cur.fetchall()

        found_records = []
        missing_ids = set(ids)

        for row in rows:
            rec_id, kind, legal, acl, data, version, create_user, create_time, modify_user, modify_time, osdu_deleted = row

            # Ensure JSON types
            if isinstance(legal, str):
                legal = json.loads(legal)
            if isinstance(acl, str):
                acl = json.loads(acl)
            if isinstance(data, str):
                data = json.loads(data)

            # Skip soft-deleted unless explicitly requested
            if osdu_deleted and not include_deleted:
                continue

            record = {
                "id": rec_id,
                "kind": kind,
                "acl": acl,
                "legal": legal,
                "data": data,
                "version": version,
                "createUser": create_user,
                "createTime": create_time.isoformat() if create_time else None,
                "modifyUser": modify_user,
                "modifyTime": modify_time.isoformat() if modify_time else None
            }
            found_records.append(record)
            missing_ids.discard(rec_id)

        return ({
            "records": found_records,
            "missingRecordIds": list(missing_ids)
        }), 200

    except Exception as e:
        logger.exception("Unhandled exception in retrieve_records")
        return ({"error": "Internal server error", "details": str(e)}), 500
    finally:
        cur.close()

# -------------------- Bulk Patch --------------------

def patch_records_bulk(patches: List[Dict]) -> Dict:
    conn = get_conn()
    record_ids, record_errors = [], []

    for patch in patches:
        record_id = patch.get("id", "<missing>")
        cur = None
        try:
            if "id" not in patch:
                record_errors.append({
                    "id": record_id,
                    "code": "VALIDATION_ERROR",
                    "reason": "Missing 'id' in patch"
                })
                continue

            cur = conn.cursor()
            cur.execute("""
                SELECT kind, legal, acl, data, version, osdu_deleted
                FROM records
                WHERE id = %s
            """, (record_id,))
            row = cur.fetchone()

            if not row:
                record_errors.append({
                    "id": record_id,
                    "code": "NOT_FOUND",
                    "reason": "Record not found"
                })
                continue

            kind, legal, acl, data, version, osdu_deleted = row
            legal = json.loads(legal) if isinstance(legal, str) else legal
            acl = json.loads(acl) if isinstance(acl, str) else acl
            data = json.loads(data) if isinstance(data, str) else data

            if osdu_deleted:
                record_errors.append({
                    "id": record_id,
                    "code": "ALREADY_DELETED",
                    "reason": "Cannot patch a deleted record"
                })
                continue

            if "data" in patch:
                data.update(patch["data"])
            if "legal" in patch:
                legal.update(patch["legal"])
            if "acl" in patch:
                acl.update(patch["acl"])
            if "kind" in patch:
                kind = patch["kind"]

            try:
                validate_data_against_schema(kind, data)
            except ValueError as ve:
                record_errors.append({
                    "id": record_id,
                    "code": "SCHEMA_VALIDATION_ERROR",
                    "reason": str(ve)
                })
                continue
            except Exception as e:
                record_errors.append({
                    "id": record_id,
                    "code": "SCHEMA_SERVICE_ERROR",
                    "reason": str(e)
                })
                continue

            now = datetime.utcnow()
            new_version = version + 1
            cur.execute("""
                UPDATE records
                SET kind = %s,
                    legal = %s,
                    acl = %s,
                    data = %s,
                    version = %s,
                    modify_user = %s,
                    modify_time = %s
                WHERE id = %s
            """, (
                kind,
                json.dumps(legal),
                json.dumps(acl),
                json.dumps(data),
                new_version,
                "system",
                now,
                record_id
            ))
            conn.commit()
            record_ids.append(record_id)

        except Exception as e:
            conn.rollback()
            logger.exception(f"Failed to patch record {record_id}")
            record_errors.append({
                "id": record_id,
                "code": "DB_ERROR",
                "reason": str(e)
            })
        finally:
            if cur:
                cur.close()

    return {
        "recordCount": len(record_ids),
        "recordIds": record_ids,
        "recordErrors": record_errors
    }
def delete_record(record_id: str) -> dict:
    """
    Soft-deletes a record by ID.
    Adds osdu_deleted and osdu_deleted_at fields to the record's data.
    """
    conn = get_conn()
    cur = conn.cursor()
    try:
        # Fetch the record
        cur.execute("""
            SELECT id, data
            FROM records
            WHERE id = %s
        """, (record_id,))
        row = cur.fetchone()

        if not row:
            logger.info(f"Record {record_id} not found")
            raise HTTPException(status_code=404, detail="Record not found")

        data = row[1]
        if isinstance(data, str):
            data = json.loads(data)

        # Mark as deleted
        data["osdu_deleted"] = True
        data["osdu_deleted_at"] = datetime.utcnow().isoformat() + "Z"

        cur.execute("""
            UPDATE records
            SET data = %s
            WHERE id = %s
        """, (json.dumps(data), record_id))

        conn.commit()
        logger.info(f"Record {record_id} soft-deleted successfully")

        return {
            "id": record_id,
            "status": "soft-deleted"
        }

    except Exception as e:
        conn.rollback()
        logger.exception(f"Unhandled exception in delete_record for {record_id}")
        raise HTTPException(status_code=500, detail=f"Internal server error: {str(e)}")
    finally:
        cur.close()

# -------------------- Flattened Records --------------------

def get_flattened_records(limit: int, offset: int) -> List[Dict]:
    query = """
        SELECT id, kind, data
        FROM records
        LIMIT %s OFFSET %s
    """

    try:
        conn = get_conn()
        with conn.cursor() as cur:
            cur.execute(query, (limit, offset))
            rows = cur.fetchall()

        results = []
        for row in rows:
            record_id, kind, data_json = row
            data = data_json if isinstance(data_json, dict) else {}
            flat_record = {
                "id": record_id,
                "kind": kind,
                **data
            }
            results.append(flat_record)

        return results

    except Exception as e:
        logger.error(f"Error in get_flattened_records: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")

def get_flattened_records_by_kind(kind: str) -> List[Dict]:
    query = """
        SELECT id, kind, data
        FROM records
        WHERE kind = %s
        LIMIT 100
    """

    try:
        conn = get_conn()
        with conn.cursor() as cur:
            cur.execute(query, (kind,))
            rows = cur.fetchall()

        results = []
        for row in rows:
            record_id, kind, data_json = row
            data = data_json if isinstance(data_json, dict) else {}
            flat_record = {
                "id": record_id,
                "kind": kind,
                **data
            }
            results.append(flat_record)

        return results

    except Exception as e:
        logger.error(f"Error in get_flattened_records_by_kind: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")

