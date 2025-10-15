# routes/schema.py
#
# Exposes HTTP endpoints for the Schema Service.

from flask import Blueprint, request, jsonify, current_app
from services.schema_service import (
    register_schema,
    get_schema_by_id,
    get_schema_by_kind
)

schema_bp = Blueprint("schema", __name__, url_prefix="/api/schema-service/v1")

# POST /api/schema-service/v1/schema
@schema_bp.route("/schema", methods=["POST"])
def post_schema():
    payload = request.get_json(silent=True)
    if not payload:
        return jsonify({"error": "Missing or invalid JSON body"}), 400

    required_fields = ["id", "kind", "status", "schema"]
    missing = [f for f in required_fields if f not in payload]
    if missing:
        return jsonify({"error": f"Missing required fields: {', '.join(missing)}"}), 400

    kind_parts = payload["kind"].split(":")
    if len(kind_parts) != 4:
        return jsonify({"error": "Invalid kind format. Expected osdu:<domain>:<entity>:<version>"}), 400

    try:
        schema_id = register_schema(payload)
        current_app.logger.info(f"✅ Registered schema: {schema_id}")
        return jsonify({"id": schema_id, "status": "registered"}), 201
    except Exception:
        current_app.logger.exception("Failed to register schema")
        return jsonify({"error": "Failed to register schema"}), 500

# GET /api/schema-service/v1/schema/<schema_id>
@schema_bp.route("/schema/<path:schema_id>", methods=["GET"])
def get_schema(schema_id):
    try:
        schema = get_schema_by_id(schema_id)
        if not schema:
            return jsonify({"error": f"Schema not found: {schema_id}"}), 404
        current_app.logger.info(f"📦 Retrieved schema by ID: {schema_id}")
        return jsonify(schema), 200
    except Exception:
        current_app.logger.exception(f"Failed to retrieve schema {schema_id}")
        return jsonify({"error": "Failed to retrieve schema"}), 500

# GET /api/schema-service/v1/schema/kind/<path:kind>
@schema_bp.route("/schema/kind/<path:kind>", methods=["GET"])
def get_schema_by_kind_route(kind):
    try:
        schema = get_schema_by_kind(kind)
        if not schema:
            return jsonify({"error": f"Schema not found for kind: {kind}"}), 404
        current_app.logger.info(f"📦 Retrieved schema by kind: {kind}")
        return jsonify(schema), 200
    except Exception:
        current_app.logger.exception(f"Failed to retrieve schema for kind {kind}")
        return jsonify({"error": "Failed to retrieve schema"}), 500

# GET /api/schema-service/v1/schema/id/<schema_id>
@schema_bp.route("/schema/id/<path:schema_id>", methods=["GET"])
def get_schema_by_id_route(schema_id):
    try:
        schema = get_schema_by_id(schema_id)
        if not schema:
            return jsonify({"error": f"Schema not found for id: {schema_id}"}), 404
        current_app.logger.info(f"📦 Retrieved schema by id: {schema_id}")
        return jsonify(schema), 200
    except Exception:
        current_app.logger.exception(f"Failed to retrieve schema for id {schema_id}")
        return jsonify({"error": "Failed to retrieve schema"}), 500


