import os
import logging
from fastapi import FastAPI, Request, HTTPException
from fastapi.responses import JSONResponse
from pydantic import BaseModel
from dotenv import load_dotenv
from opensearchpy import OpenSearch, ConnectionError

# -------------------------
# Load environment
# -------------------------
ENV_PATH = os.path.join(os.path.dirname(__file__), "opensearch.env")
load_dotenv(dotenv_path=ENV_PATH)

SEARCH_HOST = os.getenv("SEARCH_HOST", "localhost")
SEARCH_PORT = int(os.getenv("SEARCH_PORT", "9200"))
DATA_PARTITION_ID = os.getenv("DATA_PARTITION_ID", "osdu-local")
STORAGE_SERVICE_URL = os.getenv("STORAGE_SERVICE_URL")
SCHEMA_SERVICE_URL = os.getenv("SCHEMA_SERVICE_URL")
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()

# -------------------------
# Logging setup
# -------------------------
logging.basicConfig(level=LOG_LEVEL)
logger = logging.getLogger("search_service")

# -------------------------
# Connect to OpenSearch
# -------------------------
try:
    client = OpenSearch([{"host": SEARCH_HOST, "port": SEARCH_PORT}])
    info = client.info()
    logger.info("✅ Connected to OpenSearch")
    logger.debug(info)
except ConnectionError as e:
    logger.error(f"❌ Failed to connect to OpenSearch: {e}")
    raise RuntimeError("OpenSearch connection failed")

# -------------------------
# FastAPI setup
# -------------------------
app = FastAPI(title="OSDU Search Service", version="v2")

# -------------------------
# Models
# -------------------------
class IndexRecordRequest(BaseModel):
    index: str = "osdu-records"
    id: str
    document: dict

class QueryRequest(BaseModel):
    index: str = "osdu-records"
    text: str

# -------------------------
# Health check
# -------------------------
@app.get("/ping")
async def ping():
    return {"status": "ok"}

# -------------------------
# Index a record
# -------------------------
@app.post("/api/search/v2/records")
async def index_record(payload: IndexRecordRequest):
    if not payload.id or not payload.document:
        raise HTTPException(status_code=400, detail="Provide 'id' and 'document'")

    try:
        resp = client.index(index=payload.index, id=payload.id, body=payload.document, refresh=True)
        logger.info(f"Indexed record {payload.id} into {payload.index}")
        return JSONResponse(content=resp, status_code=201)
    except Exception as e:
        logger.error(f"Indexing failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))

# -------------------------
# Search records
# -------------------------
@app.post("/api/search/v2/query")
async def query_records(payload: QueryRequest):
    if not payload.text.strip():
        raise HTTPException(status_code=400, detail="Provide 'text' to search")

    try:
        result = client.search(
            index=payload.index,
            body={
                "query": {
                    "multi_match": {
                        "query": payload.text,
                        "fields": ["data.*", "kind", "id"]
                    }
                }
            }
        )
        logger.info(f"Search query executed on {payload.index}")
        return result
    except Exception as e:
        logger.error(f"Search failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))
