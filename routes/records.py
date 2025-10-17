from fastapi import APIRouter, Request, HTTPException, status
from fastapi.responses import JSONResponse
from fastapi.templating import Jinja2Templates
from pydantic import BaseModel
from typing import List, Optional
from services.record_service import (
    ingest_records,
    get_records_by_ids,
    delete_record,
    patch_record,
    ingest_records_batch,
    delete_records_bulk,
    retrieve_records,
    patch_records_bulk,
    get_flattened_records,
    get_flattened_records_by_kind,
)
import logging

router = APIRouter(prefix="/api/storage/v2", tags=["records"])
templates = Jinja2Templates(directory="templates")
logger = logging.getLogger(__name__)

# -------------------- Models --------------------

class Record(BaseModel):
    id: str
    kind: str
    acl: dict
    legal: dict
    data: dict

class BatchPayload(BaseModel):
    records: List[Record]

class DeletePayload(BaseModel):
    ids: List[str]

class RetrievePayload(BaseModel):
    records: List[str]

class PatchPayload(BaseModel):
    records: List[dict]

# -------------------- Routes --------------------

@router.put("/records")
async def put_records(records: List[Record]):
    logger.info("PUT /records route hit")
    try:
        return ingest_records([r.dict() for r in records])
    except ValueError as ve:
        logger.error(f"Validation error: {ve}")
        raise HTTPException(status_code=400, detail="VALIDATION_ERROR: " + str(ve))
    except Exception as e:
        logger.exception("Unexpected error")
        raise HTTPException(status_code=500, detail="INTERNAL_ERROR: " + str(e))

@router.get("/records")
async def get_records(request: Request, ids: str, includeDeleted: Optional[str] = "false"):
    logger.info("GET /records route hit")
    tenant_id = request.headers.get("data-partition-id")
    if not tenant_id:
        raise HTTPException(status_code=400, detail="Missing required header: data-partition-id")

    record_ids = [rid.strip() for rid in ids.split(",") if rid.strip()]
    if not record_ids:
        raise HTTPException(status_code=400, detail="No valid record IDs provided")

    return get_records_by_ids(record_ids, includeDeleted.lower() == "true")

@router.delete("/records/{record_id}")
async def delete_record_route(record_id: str, request: Request):
    tenant_id = request.headers.get("data-partition-id")
    if not tenant_id:
        raise HTTPException(status_code=400, detail="Missing required header: data-partition-id")
    return delete_record(record_id)

@router.patch("/records/{record_id}")
async def patch_record_route(record_id: str, request: Request, payload: dict):
    tenant_id = request.headers.get("data-partition-id")
    if not tenant_id:
        raise HTTPException(status_code=400, detail="Missing required header: data-partition-id")
    if not payload:
        raise HTTPException(status_code=400, detail="Missing JSON body")
    return patch_record(record_id, payload)

@router.post("/records:batch", status_code=status.HTTP_201_CREATED)
async def batch_ingest_records_route(request: Request, payload: BatchPayload):
    tenant_id = request.headers.get("data-partition-id")
    if not tenant_id:
        raise HTTPException(status_code=400, detail="Missing required header: data-partition-id")
    return ingest_records_batch([r.dict() for r in payload.records])

@router.post("/records:delete")
async def delete_records_route(request: Request, payload: DeletePayload):
    tenant_id = request.headers.get("data-partition-id")
    if not tenant_id:
        raise HTTPException(status_code=400, detail="Missing required header: data-partition-id")
    return delete_records_bulk(payload.ids)

@router.post("/records:retrieve")
async def retrieve_records_route(request: Request, payload: RetrievePayload, includeDeleted: Optional[str] = "false", latest: Optional[str] = "true"):
    tenant_id = request.headers.get("data-partition-id")
    if not tenant_id:
        raise HTTPException(status_code=400, detail="Missing required header: data-partition-id")
    return retrieve_records(payload.records, includeDeleted.lower() == "true", latest.lower() == "true")

@router.post("/records:patch")
async def patch_records_route(request: Request, payload: PatchPayload):
    tenant_id = request.headers.get("data-partition-id")
    if not tenant_id:
        raise HTTPException(status_code=400, detail="Missing required header: data-partition-id")
    return patch_records_bulk(payload.records)

@router.get("/records/flat")
async def get_flat_records(request: Request, limit: Optional[int] = 100, offset: Optional[int] = 0):
    tenant_id = request.headers.get("data-partition-id")
    if not tenant_id:
        raise HTTPException(status_code=400, detail="Missing required header: data-partition-id")
    return get_flattened_records(limit, offset)

@router.get("/records/flat/view")
async def view_flat_records(request: Request):
    return templates.TemplateResponse("flat_records.html", {"request": request})

@router.get("/records/kinds")
async def get_all_kinds():
    try:
        return get_flattened_records_by_kind("ALL_KINDS")
    except Exception as e:
        logger.error(f"Error in get_all_kinds: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")

@router.get("/records/flat/filter")
async def get_flat_records_by_kind(request: Request, kind: str):
    tenant_id = request.headers.get("data-partition-id")
    if not tenant_id:
        raise HTTPException(status_code=400, detail="Missing required header: data-partition-id")
    if not kind:
        raise HTTPException(status_code=400, detail="Missing required query parameter: kind")
    return get_flattened_records_by_kind(kind)

@router.get("/records/joined/wellbores")
async def view_joined_wellbores(request: Request):
    return templates.TemplateResponse("joined_wellbores.html", {"request": request})

@router.get("/records/schema/browser")
async def view_schema_browser(request: Request):
    return templates.TemplateResponse("schema_browser.html", {"request": request})
