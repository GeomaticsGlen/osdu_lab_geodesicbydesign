# routes/schema.py
#
# Exposes HTTP endpoints for the Schema Service.

from flask import Blueprint, request, jsonify, current_app, render_template
from db import get_conn
from services.schema_service import (
    register_schema,
    get_registered_field_types,
    get_flattened_data_fields,
    get_schema_by_id,
    get_schema_by_kind
)

schema_bp = Blueprint("schema", __name__, url_prefix="/api/schema-service/v1")

# Registers a new schema into the schema_registry table.
# Validates required fields and kind format before storing.
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
    except Exception as e:
        current_app.logger.exception(f"Failed to register schema: {e}")
        return jsonify({"error": "Failed to register schema"}), 500

# Retrieves a schema by its full ID from the schema_registry table.
# Used for direct lookup by schema ID.
@schema_bp.route("/schema/<path:schema_id>", methods=["GET"])
def get_schema(schema_id):
    try:
        schema = get_schema_by_id(schema_id)
        if not schema:
            return jsonify({"error": f"Schema not found: {schema_id}"}), 404
        current_app.logger.info(f"📦 Retrieved schema by ID: {schema_id}")
        return jsonify(schema), 200
    except Exception as e:
        current_app.logger.exception(f"Failed to retrieve schema {schema_id}: {e}")
        return jsonify({"error": "Failed to retrieve schema"}), 500

# Retrieves a schema by its kind value from the schema_registry table.
# Used by the frontend to fetch full schema definitions.
@schema_bp.route("/schema/kind/<path:kind>", methods=["GET"])
def get_schema_by_kind_route(kind):
    try:
        schema = get_schema_by_kind(kind)
        if not schema:
            return jsonify({"error": f"Schema not found for kind: {kind}"}), 404
        current_app.logger.info(f"📦 Retrieved schema by kind: {kind}")
        return jsonify(schema), 200
    except Exception as e:
        current_app.logger.exception(f"Failed to retrieve schema for kind {kind}: {e}")
        return jsonify({"error": "Failed to retrieve schema"}), 500

# Retrieves a schema by its ID using an alternate route.
# Mirrors the /schema/<id> route for compatibility.
@schema_bp.route("/schema/id/<path:schema_id>", methods=["GET"])
def get_schema_by_id_route(schema_id):
    try:
        schema = get_schema_by_id(schema_id)
        if not schema:
            return jsonify({"error": f"Schema not found for id: {schema_id}"}), 404
        current_app.logger.info(f"📦 Retrieved schema by id: {schema_id}")
        return jsonify(schema), 200
    except Exception as e:
        current_app.logger.exception(f"Failed to retrieve schema for id {schema_id}: {e}")
        return jsonify({"error": "Failed to retrieve schema"}), 500

# Returns a list of all registered master-data schema kinds from the schema_registry table.
# Used to populate the dropdown in the master schema browser UI.
@schema_bp.route("/schema/kinds", methods=["GET"])
def list_master_schema_kinds():
    try:
        conn = get_conn()
        with conn.cursor() as cur:
            cur.execute("""
                SELECT DISTINCT kind
                FROM schema_registry
                WHERE kind LIKE 'osdu:wks:master-data--%'
                ORDER BY kind
            """)
            kinds = [row[0] for row in cur.fetchall()]
        return jsonify(kinds)
    except Exception as e:
        current_app.logger.error(f"Error in list_master_schema_kinds: {e}")
        return jsonify({"error": "Internal server error"}), 500

# Serves the HTML frontend for browsing master-data schemas.
# Used by developers to inspect registered schema structures.
@schema_bp.route("/schema/browser/master", methods=["GET"])
def view_master_schema_browser():
    return render_template("master_schema_browser.html")

# Returns a flattened list of field names and types from schema_registry.schema->'schema'->'properties'->'data'.
# Used by the frontend to display simplified field structure.
@schema_bp.route("/schema/fields", methods=["GET"])
def get_schema_field_types():
    kind = request.args.get("kind")
    if not kind:
        return jsonify({"error": "Missing kind parameter"}), 400

    try:
        fields = get_registered_field_types(kind)
        return jsonify(fields)
    except Exception as e:
        current_app.logger.error(f"Error in get_schema_field_types: {e}")
        return jsonify({"error": "Internal server error"}), 500

# Returns all field names and attributes flattened from schema_registry.schema->'schema'->'properties'->'data'->'allOf'.
# Used by the frontend to display full schema field definitions for a selected master-data kind.
@schema_bp.route("/schema/fields/full", methods=["GET"])
def get_flattened_data_fields_route():
    kind = request.args.get("kind")
    if not kind:
        return jsonify({"error": "Missing kind parameter"}), 400

    try:
        fields = get_flattened_data_fields(kind)
        return jsonify(fields)
    except Exception as e:
        current_app.logger.error(f"Error in get_flattened_data_fields_route: {e}")
        return jsonify({"error": "Internal server error"}), 500
# Resolves and flattens fields from a referenced schema kind (e.g., reference-data--WellCondition).
# Used by the frontend to expand relationship fields like ConditionID into their subfields.

@schema_bp.route("/schema/resolve", methods=["GET"])
def resolve_relationship_fields_route():
    kind = request.args.get("kind")
    if not kind:
        return jsonify({"error": "Missing kind parameter"}), 400

    try:
        from services.schema_service import resolve_relationship_fields
        fields = resolve_relationship_fields(kind)
        return jsonify(fields)
    except Exception as e:
        current_app.logger.error(f"Error in resolve_relationship_fields_route: {e}")
        return jsonify({"error": "Internal server error"}), 500
# Serves the HTML frontend for browsing schema fields as a collapsible tree.
# Used to visualize nested and resolved schema fields.

@schema_bp.route("/schema/browser/tree", methods=["GET"])
def view_schema_tree_browser():
    return render_template("schema_tree_browser.html")