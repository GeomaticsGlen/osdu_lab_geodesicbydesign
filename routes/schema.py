from fastapi import APIRouter, Request, HTTPException, status, Query
from fastapi.responses import JSONResponse
from fastapi.templating import Jinja2Templates
from pydantic import BaseModel
from typing import Optional
from db import get_conn
from services.schema_service import (
    register_schema,
    get_registered_field_types,
    get_flattened_data_fields,
    get_schema_by_id,
    get_schema_by_kind
)
import logging

router = APIRouter(prefix="/api/schema-service/v1", tags=["schema"])
templates = Jinja2Templates(directory="templates")
logger = logging.getLogger(__name__)

# -------------------- Models --------------------

class SchemaRegistrationPayload(BaseModel):
    id: str
    kind: str
    status: str
    schema_definition: dict

# -------------------- Routes --------------------

@router.post("/schema", status_code=status.HTTP_201_CREATED)
async def post_schema(payload: SchemaRegistrationPayload):
    """
    Registers a new schema into the schema_registry table.
    Validates required fields and kind format before storing.
    """
    kind_parts = payload.kind.split(":")
    if len(kind_parts) != 4:
        raise HTTPException(status_code=400, detail="Invalid kind format. Expected osdu:<domain>:<entity>:<version>")

    try:
        schema_id = register_schema({
            "id": payload.id,
            "kind": payload.kind,
            "status": payload.status,
            "schema": payload.schema_definition
        })
        logger.info(f"✅ Registered schema: {schema_id}")
        return {"id": schema_id, "status": "registered"}
    except Exception as e:
        logger.exception(f"Failed to register schema: {e}")
        raise HTTPException(status_code=500, detail="Failed to register schema")

@router.get("/schema/{schema_id}")
async def get_schema(schema_id: str):
    """
    Retrieves a schema by its full ID from the schema_registry table.
    Used for direct lookup by schema ID.
    """
    try:
        schema = get_schema_by_id(schema_id)
        if not schema:
            raise HTTPException(status_code=404, detail=f"Schema not found: {schema_id}")
        logger.info(f"📦 Retrieved schema by ID: {schema_id}")
        return schema
    except Exception as e:
        logger.exception(f"Failed to retrieve schema {schema_id}: {e}")
        raise HTTPException(status_code=500, detail="Failed to retrieve schema")

@router.get("/schema/kind/{kind}")
async def get_schema_by_kind_route(kind: str):
    """
    Retrieves a schema by its kind value from the schema_registry table.
    Used by the frontend to fetch full schema definitions.
    """
    try:
        schema = get_schema_by_kind(kind)
        if not schema:
            raise HTTPException(status_code=404, detail=f"Schema not found for kind: {kind}")
        logger.info(f"📦 Retrieved schema by kind: {kind}")
        return schema
    except Exception as e:
        logger.exception(f"Failed to retrieve schema for kind {kind}: {e}")
        raise HTTPException(status_code=500, detail="Failed to retrieve schema")

@router.get("/schema/id/{schema_id}")
async def get_schema_by_id_route(schema_id: str):
    """
    Retrieves a schema by its ID using an alternate route.
    Mirrors the /schema/{id} route for compatibility.
    """
    try:
        schema = get_schema_by_id(schema_id)
        if not schema:
            raise HTTPException(status_code=404, detail=f"Schema not found for id: {schema_id}")
        logger.info(f"📦 Retrieved schema by id: {schema_id}")
        return schema
    except Exception as e:
        logger.exception(f"Failed to retrieve schema for id {schema_id}: {e}")
        raise HTTPException(status_code=500, detail="Failed to retrieve schema")

@router.get("/schema/kinds")
async def list_master_schema_kinds():
    """
    Returns a list of all registered master-data schema kinds from the schema_registry table.
    Used to populate the dropdown in the master schema browser UI.
    """
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
        return kinds
    except Exception as e:
        logger.error(f"Error in list_master_schema_kinds: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")

@router.get("/schema/browser/master")
async def view_master_schema_browser(request: Request):
    """
    Serves the HTML frontend for browsing master-data schemas.
    Used by developers to inspect registered schema structures.
    """
    return templates.TemplateResponse("master_schema_browser.html", {"request": request})

@router.get("/schema/fields")
async def get_schema_field_types(kind: str = Query(...)):
    """
    Returns a flattened list of field names and types from schema_registry.schema->'schema'->'properties'->'data'.
    Used by the frontend to display simplified field structure.
    """
    try:
        fields = get_registered_field_types(kind)
        return fields
    except Exception as e:
        logger.error(f"Error in get_schema_field_types: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")

@router.get("/schema/fields/full")
async def get_flattened_data_fields_route(kind: str = Query(...)):
    """
    Returns all field names and attributes flattened from schema_registry.schema->'schema'->'properties'->'data'->'allOf'.
    Used by the frontend to display full schema field definitions for a selected master-data kind.
    """
    try:
        fields = get_flattened_data_fields(kind)
        return fields
    except Exception as e:
        logger.error(f"Error in get_flattened_data_fields_route: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")

# @router.get("/schema/resolve")
# async def resolve_relationship_fields_route(kind: str = Query(...)):
#     """
#     Resolves and flattens fields from a referenced schema kind (e.g., reference-data--WellCondition).
#     Used by the frontend to expand relationship fields like ConditionID into their subfields.
#     """
#     try:
#         fields = resolve_relationship_fields(kind)
#         return fields
#     except Exception as e:
#         logger.error(f"Error in resolve_relationship_fields_route: {e}")
#         raise HTTPException(status_code=500, detail="Internal server error")

@router.get("/schema/browser/tree")
async def view_schema_tree_browser(request: Request):
    """
    Serves the HTML frontend for browsing schema fields as a collapsible tree.
    Used to visualize nested and resolved schema fields.
    """
    return templates.TemplateResponse("schema_tree_browser.html", {"request": request})

