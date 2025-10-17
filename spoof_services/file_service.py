import os
from fastapi import FastAPI, Request, UploadFile, File
from fastapi.responses import JSONResponse, FileResponse

# Directory to store spoofed uploads
UPLOAD_DIR = os.path.join(os.path.dirname(__file__), "uploads")
os.makedirs(UPLOAD_DIR, exist_ok=True)

app = FastAPI(title="Spoofed File Service", version="v2")

@app.get("/ping")
async def ping():
    return {"status": "ok"}

# -------------------------
# Create file metadata
# -------------------------
@app.post("/api/file/v2/files")
async def create_file(request: Request):
    body = await request.json()
    print(f"🔹 POST /api/file/v2/files → spoofed response")
    return JSONResponse(content={
        "id": "osdu:file:12345",
        "message": "spoofed file created",
        "body": body,
        "uploadUrl": "http://localhost:8005/api/file/v2/upload/osdu:file:12345",
        "downloadUrl": "http://localhost:8005/api/file/v2/download/osdu:file:12345"
    })

# -------------------------
# Get file metadata
# -------------------------
@app.get("/api/file/v2/files/{file_id}")
async def get_file(file_id: str):
    print(f"🔹 GET /api/file/v2/files/{file_id} → spoofed response")
    return {
        "id": file_id,
        "message": "spoofed file metadata",
        "signedUrl": f"http://localhost:8005/api/file/v2/download/{file_id}"
    }

# -------------------------
# Upload endpoint (simulated blob storage)
# -------------------------
@app.post("/api/file/v2/upload/{file_id}")
async def upload_file(file_id: str, file: UploadFile = File(...)):
    safe_id = file_id.replace(":", "_")   # normalise for filesystem
    file_path = os.path.join(UPLOAD_DIR, safe_id)
    with open(file_path, "wb") as f:
        f.write(await file.read())
    print(f"🔹 Uploaded file {file_id} → saved to {file_path}")
    return {"id": file_id, "message": "file uploaded successfully"}

# -------------------------
# Download endpoint (simulated blob retrieval)
# -------------------------
@app.get("/api/file/v2/download/{file_id}")
async def download_file(file_id: str):
    safe_id = file_id.replace(":", "_")   # normalise for filesystem
    file_path = os.path.join(UPLOAD_DIR, safe_id)
    if not os.path.exists(file_path):
        return JSONResponse(content={"error": "file not found"}, status_code=404)
    print(f"🔹 Downloading file {file_id}")
    return FileResponse(file_path, filename=f"{file_id}.bin")
# -------------------------
# Catch-all for other routes
# -------------------------
@app.get("/api/file/v2/{full_path:path}")
@app.post("/api/file/v2/{full_path:path}")
async def catch_all(request: Request, full_path: str):
    body = await request.json() if request.method == "POST" else {}
    print(f"🔹 {request.method} /api/file/v2/{full_path} → spoofed response")
    return JSONResponse(content={
        "message": "spoofed file response",
        "path": full_path,
        "body": body
    })
