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
    get_latest_record,
    get_specific_record_version,
    copy_record_references,
    fetch_normalized_records,
    soft_delete_single_record,
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

@router.get("/records/{record_id}")
async def get_latest_record_route(record_id: str, request: Request, attribute: Optional[List[str]] = None):
    logger.info(f"GET /records/{record_id} route hit")
    tenant_id = request.headers.get("data-partition-id")
    if not tenant_id:
        raise HTTPException(status_code=400, detail="Missing required header: data-partition-id")

    try:
        return get_latest_record(record_id, tenant_id, attribute)
    except Exception as e:
        logger.exception(f"Error fetching latest version of record {record_id}")
        raise HTTPException(status_code=500, detail=f"INTERNAL_ERROR: {str(e)}")
# Route: GET /records/{id}/{version} - fetch a specific version of a record

@router.get("/records/{record_id}/{version}")
async def get_specific_record_version_route(
    record_id: str,
    version: int,
    request: Request,
    attribute: Optional[List[str]] = None
):
    logger.info(f"GET /records/{record_id}/{version} route hit")
    tenant_id = request.headers.get("data-partition-id")
    if not tenant_id:
        raise HTTPException(status_code=400, detail="Missing required header: data-partition-id")

    try:
        return get_specific_record_version(record_id, version, tenant_id, attribute)
    except Exception as e:
        logger.exception(f"Error fetching version {version} of record {record_id}")
        raise HTTPException(status_code=500, detail=f"INTERNAL_ERROR: {str(e)}")
# Route: POST /records/{id}:delete – soft-delete a single record

@router.post("/records/{record_id}:delete")
async def soft_delete_single_record_route(record_id: str, request: Request):
    logger.info(f"POST /records/{record_id}:delete route hit")
    tenant_id = request.headers.get("data-partition-id")
    if not tenant_id:
        raise HTTPException(status_code=400, detail="Missing required header: data-partition-id")

    try:
        return soft_delete_single_record(record_id)
    except HTTPException as he:
        raise he
    except Exception as e:
        logger.exception(f"Unhandled error deleting record {record_id}")
        raise HTTPException(status_code=500, detail=f"INTERNAL_ERROR: {str(e)}")
# Route: PUT /records/copy – copy record references between namespaces

class CopyPayload(BaseModel):
    sourceNamespace: str
    targetNamespace: str
    recordIds: List[str]

@router.put("/records/copy")
async def copy_record_references_route(request: Request, payload: CopyPayload):
    logger.info(f"PUT /records/copy route hit")
    tenant_id = request.headers.get("data-partition-id")
    if not tenant_id:
        raise HTTPException(status_code=400, detail="Missing required header: data-partition-id")

    try:
        return copy_record_references(payload.sourceNamespace, payload.targetNamespace, payload.recordIds)
    except HTTPException as he:
        raise he
    except Exception as e:
        logger.exception("Unhandled error during record copy")
        raise HTTPException(status_code=500, detail=f"INTERNAL_ERROR: {str(e)}")

# Route: POST /query/records – fetch multiple records by ID

class MultiRecordPayload(BaseModel):
    recordIds: List[str]

@router.post("/query/records")
async def fetch_multiple_records_route(request: Request, payload: MultiRecordPayload):
    logger.info("POST /query/records route hit")
    tenant_id = request.headers.get("data-partition-id")
    if not tenant_id:
        raise HTTPException(status_code=400, detail="Missing required header: data-partition-id")

    try:
        return get_records_by_ids(payload.recordIds)
    except HTTPException as he:
        raise he
    except Exception as e:
        logger.exception("Unhandled error fetching multiple records")
        raise HTTPException(status_code=500, detail=f"INTERNAL_ERROR: {str(e)}")
# Route: POST /query/records:batch – fetch multiple records with normalization context

class NormalizedRecordPayload(BaseModel):
    recordIds: List[str]

@router.post("/query/records:batch")
async def fetch_normalized_records_route(
    request: Request,
    payload: NormalizedRecordPayload
):
    logger.info("POST /query/records:batch route hit")
    tenant_id = request.headers.get("data-partition-id")
    frame_of_reference = request.headers.get("frame-of-reference")

    if not tenant_id:
        raise HTTPException(status_code=400, detail="Missing required header: data-partition-id")
    if not frame_of_reference:
        raise HTTPException(status_code=400, detail="Missing required header: frame-of-reference")

    try:
        return fetch_normalized_records(payload.recordIds, frame_of_reference)
    except HTTPException as he:
        raise he
    except Exception as e:
        logger.exception("Unhandled error fetching normalized records")
        raise HTTPException(status_code=500, detail=f"INTERNAL_ERROR: {str(e)}")
