from fastapi import FastAPI, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
import logging
import os
from typing import Any, Dict

from fabric_client import FabricClient

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("customer360-backend")

app = FastAPI(
    title="Customer360 Conversational Analytics Backend",
    description="Backend API for AI-powered customer analytics",
    version="1.0.0"
)

allowed_origins = os.environ.get("CORS_ALLOWED_ORIGINS", "*").split(",")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"] if "*" in allowed_origins else allowed_origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

try:
    fabric_client = FabricClient()
    logger.info("✓ Fabric Data Agent client initialized")
except Exception as ex:
    logger.error(f"Failed to initialize Fabric client: {ex}")
    fabric_client = None

@app.get("/")
async def root() -> Dict[str, str]:
    return {
        "service": "Customer360 Conversational Analytics",
        "status": "running",
        "version": "1.0.0"
    }

@app.get("/health")
async def health() -> Dict[str, str]:
    return {"status": "healthy"}

@app.get("/api/config")
async def config() -> Dict[str, str]:
    """
    Returns runtime configuration for the frontend.
    Exposes POWERBI_REPORT_URL as a backend App Service setting so the Power BI
    embed URL can be updated without rebuilding the frontend Docker image.
    """
    return {
        "powerbi_report_url": os.environ.get("POWERBI_REPORT_URL", ""),
    }

@app.post("/api/chat")
async def chat(request: Request) -> Dict[str, Any]:
    if not fabric_client:
        raise HTTPException(status_code=503, detail="Fabric client not configured")

    try:
        body = await request.json()
        message = body.get("message")
        user_id = body.get("userId", "anonymous")

        if not message:
            raise HTTPException(status_code=400, detail="'message' field is required")

        logger.info(f"Chat request from {user_id}: {message[:100]}")
        result = fabric_client.chat(user_id=user_id, message=message)
        logger.info(f"Chat response received for {user_id}")

        return {
            "answer": result.get("answer", ""),
            "timestamp": result.get("timestamp"),
            "metadata": result.get("metadata", {})
        }
    except HTTPException:
        raise
    except Exception as ex:
        logger.exception(f"Chat error: {ex}")
        raise HTTPException(status_code=500, detail=f"Internal server error: {str(ex)}")

if __name__ == "__main__":
    import uvicorn
    port = int(os.environ.get("PORT", 8000))
    uvicorn.run(app, host="0.0.0.0", port=port)
