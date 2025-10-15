###backend/schema_app.py####
import os
import json
import sys
from typing import Any, Dict, Set

from flask import Flask, jsonify, request
import psycopg2
from psycopg2.extras import RealDictCursor
from dotenv import load_dotenv

# Load environment variables
load_dotenv(dotenv_path=os.path.join(os.path.dirname(__file__), "schemaservice.env"))

app = Flask(__name__)

# -----------------------
# Database access helpers
# -----------------------
def get_db_connection():
    return psycopg2.connect(
        dbname=os.getenv("SCHEMA_DB_NAME"),
        user=os.getenv("SCHEMA_DB_USER"),
        password=os.getenv("SCHEMA_DB_PASSWORD"),
        host=os.getenv("SCHEMA_DB_HOST"),
        port=os.getenv("SCHEMA_DB_PORT"),
    )

def fetch_schema_doc(schema_id: str) -> Dict[str, Any]:
    """Fetch the raw schema document (wrapper) with keys: schema, schemaInfo."""
    conn = get_db_connection()
    cur = conn.cursor(cursor_factory=RealDictCursor)
    cur.execute("SELECT schema FROM schema_registry WHERE id = %s", (schema_id,))
    row = cur.fetchone()
    cur.close()
    conn.close()
    if not row:
        return None
    doc = row["schema"]
    if isinstance(doc, str):
        doc = json.loads(doc)
    if not isinstance(doc, dict) or "schema" not in doc:
        doc = {"schema": doc, "schemaInfo": {"id": schema_id}}
    return doc

# -----------------------
# Resolver core
# -----------------------
class SchemaResolver:
    def __init__(self):
        self._cache: Dict[str, Dict[str, Any]] = {}

    def resolve_by_id(self, schema_id: str) -> Dict[str, Any]:
        if schema_id in self._cache:
            return self._cache[schema_id]

        doc = fetch_schema_doc(schema_id)
        if not doc:
            raise ValueError(f"Unresolvable: {schema_id}")

        resolved = self._resolve_doc(doc, visited=set())
        self._cache[schema_id] = resolved
        return resolved

    def _resolve_doc(self, doc: Dict[str, Any], visited: Set[str]) -> Dict[str, Any]:
        schema = json.loads(json.dumps(doc.get("schema", {})))
        schema_info = doc.get("schemaInfo", {})

        schema = self._merge_inheritance(schema, visited)
        schema = self._expand_refs(schema, visited)

        return {"schema": schema, "schemaInfo": schema_info}

    def _merge_inheritance(self, schema: Dict[str, Any], visited: Set[str]) -> Dict[str, Any]:
        inherits = schema.get("x-osdu-inheriting-from-kind", [])
        if not isinstance(inherits, list):
            inherits = []

        for parent_id in inherits:
            print(f"[resolver] merging parent {parent_id}", file=sys.stderr)
            parent_doc = fetch_schema_doc(parent_id)
            if not parent_doc:
                raise ValueError(f"Unresolvable: {parent_id}")

            if parent_id in visited:
                continue
            visited.add(parent_id)

            parent_resolved = self._resolve_doc(parent_doc, visited)
            parent_schema = parent_resolved.get("schema", {})

            child_props = schema.setdefault("properties", {})
            parent_props = parent_schema.get("properties", {})
            for k, v in parent_props.items():
                if k not in child_props:
                    child_props[k] = v

            child_required = schema.setdefault("required", [])
            parent_required = parent_schema.get("required", [])
            for r in parent_required:
                if r not in child_required:
                    child_required.append(r)

        schema.pop("x-osdu-inheriting-from-kind", None)
        return schema

    def _expand_refs(self, node: Any, visited: Set[str]) -> Any:
        if isinstance(node, dict):
            if "$ref" in node and isinstance(node["$ref"], str):
                ref_id = node["$ref"]
                print(f"[resolver] expanding $ref {ref_id}", file=sys.stderr)
                if ref_id in self._cache:
                    return self._cache[ref_id].get("schema", {})

                ref_doc = fetch_schema_doc(ref_id)
                if not ref_doc:
                    raise ValueError(f"Unresolvable: {ref_id}")

                if ref_id in visited:
                    cached = self._cache.get(ref_id)
                    return cached.get("schema", {}) if cached else {}

                visited.add(ref_id)
                resolved_target = self._resolve_doc(ref_doc, visited)
                self._cache[ref_id] = resolved_target
                return resolved_target.get("schema", {})

            return {k: self._expand_refs(v, visited) for k, v in node.items()}

        if isinstance(node, list):
            return [self._expand_refs(i, visited) for i in node]

        return node

resolver = SchemaResolver()

# -----------------------
# Flask route
# -----------------------
@app.route("/api/schema-service/v1/schemas/<path:schema_id>", methods=["GET"])
def get_schema(schema_id: str):
    doc = fetch_schema_doc(schema_id)
    if not doc:
        return jsonify({"error": f"Schema {schema_id} not found"}), 404

    if request.args.get("resolve") == "true":
        try:
            resolved = resolver.resolve_by_id(schema_id)
            return jsonify(resolved["schema"]), 200
        except ValueError as e:
            return jsonify({"error": str(e)}), 400

    return jsonify(doc), 200

if __name__ == "__main__":
    app.run(port=5001, debug=True)
