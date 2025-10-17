# Changelog

## [Unreleased] - 2025-10-17

### 🔥 Flask → FastAPI Migration

#### ✅ Core Migration
- Replaced Flask app with FastAPI in `app.py`
  - Introduced `FastAPI()` instance with CORS middleware
  - Registered routers from `routes/records.py` and `routes/schema.py`
  - Configured structured logging and route introspection
  - Enabled Swagger UI (`/docs`) and Redoc (`/redoc`) for API exploration

#### ✅ Route Refactoring
- Converted all Flask blueprints to FastAPI routers:
  - `routes/records.py`: now uses `APIRouter`, typed request models, and HTTPException handling
  - `routes/schema.py`: updated to use FastAPI decorators and OpenAPI-compatible models

#### ✅ Service Layer Refactoring
- Fully refactored `services/record_service.py` and `services/schema_service.py`:
  - Removed all Flask dependencies (`jsonify`, `current_app`)
  - Replaced with `logging.getLogger(__name__)` and native Python dict responses
  - Replaced Flask error handling with `fastapi.HTTPException`
  - Ensured all service functions are modular, reproducible, and Swagger-compatible

#### ✅ Model Cleanup
- Renamed `schema` field in `SchemaRegistrationPayload` to `schema_definition` to avoid shadowing `BaseModel.schema()`
  - Updated route and service logic to preserve DB structure while resolving Swagger warning

#### ✅ Swagger & OpenAPI Compliance
- All endpoints now auto-documented via FastAPI’s OpenAPI schema
- Request/response models validated and exposed in Swagger UI
- Verified route registration and introspection via structured logging

---
🔄 Changelog: New OSDU Storage API Routes (FastAPI)
✅ Implemented Routes
GET /records/{id} Fetches the latest version of a record by ID.

GET /records/{id}/{version} Retrieves a specific version of a record.

POST /records/{id}:delete Soft-deletes a single record (logical deletion with audit trail).

PUT /records/copy Copies record references from one namespace to another. All-or-nothing transactional behavior.

POST /query/records Fetches multiple records by ID in a single request.

POST /query/records:batch Fetches multiple records with normalization context (frame-of-reference header).
