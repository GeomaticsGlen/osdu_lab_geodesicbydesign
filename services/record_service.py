#record_service.py
def ingest_records(records):
    conn = get_conn()
    ingested_ids, record_errors = [], []

    for record in records:
        record_id = record.get("id", "<missing>")
        cur = None
        try:
            # Validate required fields + schema
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
            current_app.logger.exception(f"Failed to ingest record {record_id}")
            record_errors.append({
                "id": record_id,
                "code": "DB_ERROR",
                "reason": str(e)
            })
        finally:
            if cur:
                cur.close()

    if not ingested_ids:
        current_app.logger.warning("❌ No records were committed to the database.")
        return jsonify({
            "error": "NO_RECORDS_COMMITTED",
            "reason": "All records failed validation or DB insert",
            "recordErrors": record_errors
        }), 400

    return jsonify({
        "recordCount": len(ingested_ids),
        "recordIds": ingested_ids,
        "recordErrors": record_errors
    }), 201


def get_records_by_ids(record_ids, include_deleted=False):
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

            # Ensure JSON types
            if isinstance(legal, str):
                legal = json.loads(legal)
            if isinstance(acl, str):
                acl = json.loads(acl)
            if isinstance(data, str):
                data = json.loads(data)

            # Skip soft-deleted unless explicitly requested
            if data.get("osdu_deleted") is True and not include_deleted:
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

        return jsonify({
            "records": found_records,
            "missingRecordIds": list(missing_ids)
        }), 200

    except Exception as e:
        current_app.logger.exception("Unhandled exception in get_records_by_ids")
        return jsonify({"error": "Internal server error", "details": str(e)}), 500
    finally:
        cur.close()
# ------------------------------------------------------------------------------
# Service: delete_record
#
# Handles the soft-delete of a record by ID. Updates the record's data to include
# osdu_deleted and osdu_deleted_at fields. Returns a structured JSON response.
# ------------------------------------------------------------------------------

def delete_record(record_id):
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
            current_app.logger.info(f"Record {record_id} not found")
            return jsonify({"error": "Record not found"}), 404

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
        current_app.logger.info(f"Record {record_id} soft-deleted successfully")

        return jsonify({
            "id": record_id,
            "status": "soft-deleted"
        }), 200

    except Exception as e:
        conn.rollback()
        current_app.logger.exception(f"Unhandled exception in delete_record for {record_id}")
        return jsonify({"error": "Internal server error", "details": str(e)}), 500
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
            return jsonify({"error": "Record not found"}), 404

        kind, legal, acl, data, version, osdu_deleted = row

        # Ensure JSON types
        if isinstance(legal, str):
            legal = json.loads(legal)
        if isinstance(acl, str):
            acl = json.loads(acl)
        if isinstance(data, str):
            data = json.loads(data)

        if osdu_deleted:
            return jsonify({
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
            return jsonify({"error": "SCHEMA_VALIDATION_ERROR", "reason": str(ve)}), 400
        except Exception as e:
            return jsonify({"error": "SCHEMA_SERVICE_ERROR", "reason": str(e)}), 500

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

        return jsonify({
            "id": record_id,
            "status": "patched",
            "updated_fields": list(payload.keys())
        }), 200

    except Exception as e:
        conn.rollback()
        current_app.logger.exception(f"Unhandled exception in patch_record for {record_id}")
        return jsonify({"error": "Internal server error", "details": str(e)}), 500
    finally:
        cur.close()
# ------------------------------------------------------------------------------
# Service: ingest_records_batch
#
# Handles ingestion of multiple records in one request. Validates required fields,
# checks schema compliance, and inserts or updates records with versioning.
# ------------------------------------------------------------------------------
from services.schema_service import validate_record

def ingest_records_batch(records):
    conn = get_conn()
    record_ids, record_errors = [], []

    for record in records:
        record_id = record.get("id", "<missing>")
        cur = None
        try:
            # Validate required fields + schema
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
            current_app.logger.exception(f"Failed to ingest record {record_id}")
            record_errors.append({
                "id": record_id,
                "code": "DB_ERROR",
                "reason": str(e)
            })
        finally:
            if cur:
                cur.close()

    if not record_ids:
        current_app.logger.warning("❌ No records were committed to the database.")
        return jsonify({
            "error": "NO_RECORDS_COMMITTED",
            "reason": "All records failed validation or DB insert",
            "recordErrors": record_errors
        }), 400

    return jsonify({
        "recordCount": len(record_ids),
        "recordIds": record_ids,
        "recordErrors": record_errors
    }), 201

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
            current_app.logger.exception(f"Failed to delete record {rid}")
            record_errors.append({
                "id": rid,
                "code": "DB_ERROR",
                "reason": str(e)
            })
        finally:
            if cur:
                cur.close()

    return jsonify({
        "recordCount": len(record_ids),
        "recordIds": record_ids,
        "recordErrors": record_errors
    }), 200
# ------------------------------------------------------------------------------
# Service: retrieve_records
#
# Handles retrieval of multiple records by ID. Returns full payload, version,
# and audit metadata. Supports includeDeleted and latest flags.
# ------------------------------------------------------------------------------

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

        return jsonify({
            "records": found_records,
            "missingRecordIds": list(missing_ids)
        }), 200

    except Exception as e:
        current_app.logger.exception("Unhandled exception in retrieve_records")
        return jsonify({"error": "Internal server error", "details": str(e)}), 500
    finally:
        cur.close()
# ------------------------------------------------------------------------------
# Service: patch_records_bulk
#
# Handles partial updates to multiple records. Merges incoming fields into the
# existing record, validates against schema, and updates the DB with a new version.
# ------------------------------------------------------------------------------
import json
from datetime import datetime
from flask import jsonify, current_app
from db import get_conn
from services.schema_service import validate_data_against_schema

def patch_records_bulk(patches):
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

            # Ensure JSON types
            if isinstance(legal, str):
                legal = json.loads(legal)
            if isinstance(acl, str):
                acl = json.loads(acl)
            if isinstance(data, str):
                data = json.loads(data)

            if osdu_deleted:
                record_errors.append({
                    "id": record_id,
                    "code": "ALREADY_DELETED",
                    "reason": "Cannot patch a deleted record"
                })
                continue

            # Merge patch into existing data
            if "data" in patch:
                data.update(patch["data"])
            if "legal" in patch:
                legal.update(patch["legal"])
            if "acl" in patch:
                acl.update(patch["acl"])
            if "kind" in patch:
                kind = patch["kind"]

            # Schema validation
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
            record_ids.append(record_id)

        except Exception as e:
            conn.rollback()
            current_app.logger.exception(f"Failed to patch record {record_id}")
            record_errors.append({
                "id": record_id,
                "code": "DB_ERROR",
                "reason": str(e)
            })
        finally:
            if cur:
                cur.close()

    return jsonify({
        "recordCount": len(record_ids),
        "recordIds": record_ids,
        "recordErrors": record_errors
    }), 200


