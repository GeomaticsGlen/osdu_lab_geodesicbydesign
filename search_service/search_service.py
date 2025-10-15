import os
from flask import Flask, request, jsonify
from dotenv import load_dotenv
from opensearchpy import OpenSearch

# Load .env file
load_dotenv(dotenv_path="C:/Users/Stuart G/Documents/osdu_postgres/search_service/.env")

app = Flask(__name__)

# Read values from .env
SEARCH_HOST = os.getenv("SEARCH_HOST", "localhost")
SEARCH_PORT = int(os.getenv("SEARCH_PORT", "9200"))
DATA_PARTITION_ID = os.getenv("DATA_PARTITION_ID", "osdu-local")
STORAGE_SERVICE_URL = os.getenv("STORAGE_SERVICE_URL")
SCHEMA_SERVICE_URL = os.getenv("SCHEMA_SERVICE_URL")

# Connect to OpenSearch
client = OpenSearch([{"host": SEARCH_HOST, "port": SEARCH_PORT}])

# -------------------------
# Health check
# -------------------------
@app.route("/ping", methods=["GET"])
def ping():
    return jsonify({"status": "ok"}), 200

# -------------------------
# Index a record
# -------------------------
@app.route("/api/search/v2/records", methods=["POST"])
def index_record():
    body = request.get_json(force=True)
    index = body.get("index", "osdu-records")
    rid = body.get("id")
    doc = body.get("document")

    if not rid or not doc:
        return jsonify({"error": "Provide 'id' and 'document'"}), 400

    try:
        resp = client.index(index=index, id=rid, body=doc, refresh=True)
        return jsonify(resp), 201
    except Exception as e:
        return jsonify({"error": str(e)}), 500

# -------------------------
# Search records
# -------------------------
@app.route("/api/search/v2/query", methods=["POST"])
def query_records():
    body = request.get_json(force=True)
    index = body.get("index", "osdu-records")
    text = body.get("text", "").strip()

    if not text:
        return jsonify({"error": "Provide 'text' to search"}), 400

    try:
        result = client.search(
            index=index,
            body={
                "query": {
                    "multi_match": {
                        "query": text,
                        "fields": ["data.*", "kind", "id"]
                    }
                }
            }
        )
        return jsonify(result), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500

# -------------------------
# Main entry
# -------------------------
if __name__ == "__main__":
    # Default to port 5003 for Search Service
    app.run(host="0.0.0.0", port=5003, debug=True)
