from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse

app = FastAPI(title="Spoofed Entitlements Service", version="v2")

@app.get("/ping")
async def ping():
    return {"status": "ok"}

@app.get("/api/entitlements/v2/{full_path:path}")
@app.post("/api/entitlements/v2/{full_path:path}")
async def spoof_entitlements(request: Request, full_path: str):
    body = await request.json() if request.method == "POST" else {}
    print(f"🔹 {request.method} /api/entitlements/v2/{full_path} → spoofed response")
    return JSONResponse(content={
        "message": "spoofed entitlements response",
        "path": full_path,
        "body": body
    })
