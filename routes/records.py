#routes/records.py
from db import get_conn  # ✅ db.py is at repo root
from services.record_service import get_flattened_records
from services.record_service import get_flattened_records_by_kind
from flask import Blueprint, request, jsonify, current_app
from services.record_service import (
    ingest_records,
    get_records_by_ids,
    delete_record,
    patch_record,
    ingest_records_batch,
    delete_records_bulk,
    retrieve_records,
    patch_records_bulk,
)
records_bp = Blueprint("records", __name__, url_prefix="/api/storage/v2")

# @records_bp.route("/records", methods=["PUT"])
# def put_records():
#     current_app.logger.info("PUT /records route hit")
#     payload = request.get_json()
#     if not isinstance(payload, list):
#         return jsonify({"error": "Payload must be a list of records"}), 400
#     return ingest_records(payload)
@records_bp.route("/records", methods=["PUT"])
def put_records():
    current_app.logger.info("PUT /records route hit")
    payload = request.get_json()

    if not isinstance(payload, list):
        return jsonify({"error": "Payload must be a list of records"}), 400

    try:
        return ingest_records(payload)
    except ValueError as ve:
        current_app.logger.error(f"❌ Validation error in PUT /records: {ve}")
        return jsonify({"error": "VALIDATION_ERROR", "reason": str(ve)}), 400
    except Exception as e:
        current_app.logger.exception("❌ Unexpected error in PUT /records")
        return jsonify({"error": "INTERNAL_ERROR", "reason": str(e)}), 500

@records_bp.route("/records", methods=["GET"])
def get_records():
    current_app.logger.info("GET /records route hit")

    tenant_id = request.headers.get("data-partition-id")
    if not tenant_id:
        return jsonify({"error": "Missing required header: data-partition-id"}), 400

    ids_param = request.args.get("ids")
    include_deleted = request.args.get("includeDeleted", "false").lower() == "true"

    if not ids_param:
        return jsonify({"error": "Missing required query parameter: ids"}), 400

    record_ids = [rid.strip() for rid in ids_param.split(",") if rid.strip()]
    if not record_ids:
        return jsonify({"error": "No valid record IDs provided"}), 400

    return get_records_by_ids(record_ids, include_deleted)

# ------------------------------------------------------------------------------
# DELETE /api/storage/v2/records/<record_id>
#
# Purpose:
# Soft-delete a single record by ID. Marks the record as deleted by setting
# osdu_deleted=true and osdu_deleted_at timestamp. Does not physically remove
# the row from the database.
#
# Behavior:
# - Requires 'data-partition-id' header
# - Returns 400 if header missing
# - Returns 404 if record not found
# - Returns 200 with status if successfully soft-deleted
# - Returns 500 for internal errors
# ------------------------------------------------------------------------------
@records_bp.route("/records/<record_id>", methods=["DELETE"])
def delete_record_route(record_id):
    tenant_id = request.headers.get("data-partition-id")
    if not tenant_id:
        return jsonify({"error": "Missing required header: data-partition-id"}), 400
    return delete_record(record_id)
# ------------------------------------------------------------------------------
# PATCH /api/storage/v2/records/<record_id>
#
# Purpose:
# Apply partial updates to a single record. Merges provided fields into the
# existing record and validates the result against its schema.
#
# Behavior:
# - Requires 'data-partition-id' header
# - Accepts a JSON body with any subset of: kind, legal, acl, data
# - Fetches the existing record from PostgreSQL
# - Merges updates into the current record
# - Resolves the schema for the (possibly updated) kind
# - Validates the merged 'data' against the resolved schema
# - Commits the update if validation passes
# - Returns 400 for missing headers or validation errors
# - Returns 404 if the record does not exist
# - Returns 500 for internal errors
# ------------------------------------------------------------------------------
@records_bp.route("/records/<record_id>", methods=["PATCH"])
def patch_record_route(record_id):
    tenant_id = request.headers.get("data-partition-id")
    if not tenant_id:
        return jsonify({"error": "Missing required header: data-partition-id"}), 400

    payload = request.get_json()
    if not payload:
        return jsonify({"error": "Missing JSON body"}), 400

    return patch_record(record_id, payload)
# ------------------------------------------------------------------------------
# POST /api/storage/v2/records:batch
#
# Purpose:
# Ingest multiple records in a single request. Each record is validated against
# its declared schema using the local schema service.
#
# Behavior:
# - Requires 'data-partition-id' header
# - Accepts a JSON body with a 'records' array
# - Validates required fields: id, kind, legal, acl, data
# - Resolves and validates each record's 'data' against its schema
# - Inserts or updates records in PostgreSQL with versioning
# - Returns structured response with ingested IDs and per-record errors
# - Returns 201 Created on success
# ------------------------------------------------------------------------------
@records_bp.route("/records:batch", methods=["POST"])
def batch_ingest_records_route():
    tenant_id = request.headers.get("data-partition-id")
    if not tenant_id:
        return jsonify({"error": "Missing required header: data-partition-id"}), 400

    payload = request.get_json()
    if not payload or "records" not in payload:
        return jsonify({"error": "Payload must include 'records' array"}), 400

    records = payload["records"]
    if not isinstance(records, list):
        return jsonify({"error": "'records' must be a list"}), 400

    return ingest_records_batch(records)

# ------------------------------------------------------------------------------
# POST /api/storage/v2/records:delete
#
# Purpose:
# Soft-delete multiple records in a single request. Marks records as deleted by
# setting osdu_deleted=true and osdu_deleted_at timestamp. Does not physically
# remove rows from the database.
#
# Behavior:
# - Requires 'data-partition-id' header
# - Accepts a JSON body with an 'ids' array
# - For each ID:
#   - Returns NOT_FOUND if record does not exist
#   - Returns ALREADY_DELETED if record already marked deleted
#   - Otherwise marks record as deleted and updates audit metadata
# - Returns structured response with recordCount, recordIds, recordErrors
# - Returns 200 OK on completion
# ------------------------------------------------------------------------------
@records_bp.route("/records:delete", methods=["POST"])
def delete_records_route():
    tenant_id = request.headers.get("data-partition-id")
    if not tenant_id:
        return jsonify({"error": "Missing required header: data-partition-id"}), 400

    payload = request.get_json()
    if not payload or "ids" not in payload:
        return jsonify({"error": "Payload must include 'ids' array"}), 400

    ids = payload["ids"]
    if not isinstance(ids, list) or not ids:
        return jsonify({"error": "'ids' must be a non-empty list"}), 400

    return delete_records_bulk(ids)
# ------------------------------------------------------------------------------
# POST /api/storage/v2/records:retrieve
#
# Purpose:
# Retrieve multiple records by ID in a single request. Returns full payload,
# version, and audit metadata. Supports flags for including deleted records
# and returning only the latest version.
#
# Behavior:
# - Requires 'data-partition-id' header
# - Accepts a JSON body with a 'records' array of IDs
# - Query flags:
#   - includeDeleted=true|false (default false)
#   - latest=true|false (default true)
# - Returns structured response with:
#   - records: array of found records
#   - missingRecordIds: array of IDs not found (or hidden if deleted)
# - Returns 400 for malformed payloads
# - Returns 500 for internal errors
# ------------------------------------------------------------------------------
@records_bp.route("/records:retrieve", methods=["POST"])
def retrieve_records_route():
    tenant_id = request.headers.get("data-partition-id")
    if not tenant_id:
        return jsonify({"error": "Missing required header: data-partition-id"}), 400

    payload = request.get_json()
    if not payload or "records" not in payload:
        return jsonify({"error": "Payload must include 'records' array"}), 400

    ids = payload["records"]
    if not isinstance(ids, list) or not ids:
        return jsonify({"error": "'records' must be a non-empty list"}), 400

    include_deleted = request.args.get("includeDeleted", "false").lower() == "true"
    latest_only = request.args.get("latest", "true").lower() == "true"

    return retrieve_records(ids, include_deleted, latest_only)
# ------------------------------------------------------------------------------
# POST /api/storage/v2/records:patch
#
# Purpose:
# Apply partial updates to multiple existing records. Each patch merges fields
# into the current record and validates the result against its declared schema.
#
# Behavior:
# - Requires 'data-partition-id' header
# - Accepts a JSON body with a 'records' array of patch objects
# - For each record:
#   - Fetches the existing record by ID
#   - Merges patch fields: data, legal, acl
#   - Validates the merged 'data' against the resolved schema
#   - Skips records marked as deleted
# - Returns structured response with updated IDs and per-record errors
# - Returns 400 for malformed payloads
# - Returns 500 for internal errors
# ------------------------------------------------------------------------------
@records_bp.route("/records:patch", methods=["POST"])
def patch_records_route():
    tenant_id = request.headers.get("data-partition-id")
    if not tenant_id:
        return jsonify({"error": "Missing required header: data-partition-id"}), 400

    payload = request.get_json()
    if not payload or "records" not in payload:
        return jsonify({"error": "Payload must include 'records' array"}), 400

    patches = payload["records"]
    if not isinstance(patches, list) or not patches:
        return jsonify({"error": "'records' must be a non-empty list"}), 400

    return patch_records_bulk(patches)
# ----------------------------------------------------------------------
# Route: GET /records/flat
# Purpose: Returns records with 'data' JSONB fields unpacked into top-level keys.
# Notes:
# - Validates 'data-partition-id' header
# - Supports pagination via 'limit' and 'offset'
# - Delegates logic to get_flattened_records() in record_service.py
# ----------------------------------------------------------------------
@records_bp.route("/records/flat", methods=["GET"])
def get_flat_records():
    current_app.logger.info("GET /records/flat route hit")

    tenant_id = request.headers.get("data-partition-id")
    if not tenant_id:
        return jsonify({"error": "Missing required header: data-partition-id"}), 400

    limit = request.args.get("limit", type=int, default=100)
    offset = request.args.get("offset", type=int, default=0)

    return get_flattened_records(limit, offset)
from flask import render_template

@records_bp.route("/records/flat/view", methods=["GET"])
# ----------------------------------------------------------------------
# Route: GET /records/flat/view
# Purpose: Serves a simple HTML page that displays flattened records in a table.
# ----------------------------------------------------------------------
def view_flat_records():
    return render_template("flat_records.html")

@records_bp.route("/records/kinds", methods=["GET"])
# ----------------------------------------------------------------------
# Route: GET /records/kinds
# Purpose: Returns a list of distinct 'kind' values from the records table.
# ----------------------------------------------------------------------
def get_all_kinds():
    current_app.logger.info("GET /records/kinds route hit")

    try:
        conn = get_conn()
        with conn.cursor() as cur:
            cur.execute("SELECT DISTINCT kind FROM records ORDER BY kind ASC")
            rows = cur.fetchall()

        kinds = [row[0] for row in rows]
        return jsonify(kinds)

    except Exception as e:
        current_app.logger.error(f"Error in get_all_kinds: {e}")
        return jsonify({"error": "Internal server error"}), 500

@records_bp.route("/records/flat/filter", methods=["GET"])
# ----------------------------------------------------------------------
# Route: GET /records/flat/filter
# Purpose: Returns flattened records filtered by 'kind' value.
# ----------------------------------------------------------------------
def get_flat_records_by_kind():
    current_app.logger.info("GET /records/flat/filter route hit")

    tenant_id = request.headers.get("data-partition-id")
    kind = request.args.get("kind")

    if not tenant_id:
        return jsonify({"error": "Missing required header: data-partition-id"}), 400
    if not kind:
        return jsonify({"error": "Missing required query parameter: kind"}), 400

    return get_flattened_records_by_kind(kind)

@records_bp.route("/records/joined/wellbores", methods=["GET"])
def view_joined_wellbores():
    return render_template("joined_wellbores.html")

@records_bp.route("/records/schema/browser", methods=["GET"])
def view_schema_browser():
    return render_template("schema_browser.html")






