# ------------------------------------------------------------------------------
# app.py
#
# Purpose:
# Entry point for the OSDU Storage + Schema Service FastAPI application.
# - Creates the FastAPI app instance
# - Configures CORS
# - Loads environment variables (e.g. DB credentials from osdudb.env)
# - Registers routers (routes)
#
# This file should remain minimal — all business logic lives in services/,
# and all HTTP routes live in routes/.
# ------------------------------------------------------------------------------

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from dotenv import load_dotenv
import logging
from routes.records import router as records_router
from routes.schema import router as schema_router

# Load environment variables from backend/osdudb.env
load_dotenv("backend/osdudb.env")

# Create FastAPI app
app = FastAPI(
    title="OSDU Storage + Schema Service",
    version="1.0.0",
    description="FastAPI backend for OSDU record and schema management"
)

# Configure CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Adjust for production
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="[%(asctime)s] %(levelname)s in %(module)s: %(message)s"
)
logger = logging.getLogger(__name__)

# Register routers
app.include_router(records_router)
app.include_router(schema_router)

# Log all registered routes
for route in app.routes:
    logger.info(f"[ROUTE] {route.name}: {route.methods} -> {route.path}")

# Run the app
if __name__ == "__main__":
    import uvicorn
    uvicorn.run("app:app", host="0.0.0.0", port=5000, reload=True)
