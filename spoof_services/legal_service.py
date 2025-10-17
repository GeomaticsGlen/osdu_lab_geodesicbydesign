from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse

app = FastAPI(title="Spoofed Legal Service", version="v1")

# Explicit health check route
@app.get("/api/legal/v1/ping")
async def ping():
    return {"status": "ok"}

# Catch-all GET route for spoofed responses
@app.get("/api/legal/v1/{full_path:path}")
async def spoof_legal_get(full_path: str):
    print(f"🔹 GET /api/legal/v1/{full_path} → spoofed response")
    return JSONResponse(content={
        "message": "spoofed legal response",
        "path": full_path,
        "body": {}
    })

# Catch-all POST route for spoofed responses
@app.post("/api/legal/v1/{full_path:path}")
async def spoof_legal_post(request: Request, full_path: str):
    body = await request.json()
    print(f"🔹 POST /api/legal/v1/{full_path} → spoofed response")
    return JSONResponse(content={
        "message": "spoofed legal response",
        "path": full_path,
        "body": body
    })

