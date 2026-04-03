import httpx
import os
import logging
from fastapi import APIRouter, Request, HTTPException, Depends
from datastore.supabase_client import SUPABASE_URL, SUPABASE_ANON_KEY

logger = logging.getLogger(__name__)
router = APIRouter()

async def validate_access_token(request: Request):
    auth_header = request.headers.get("Authorization")
    if not auth_header or not auth_header.startswith("Bearer "):
        raise HTTPException(status_code=401, detail="Missing or invalid Authorization header")

    if not SUPABASE_URL or not SUPABASE_ANON_KEY:
        logger.error("Supabase configuration missing for token validation in protected router.")
        raise HTTPException(status_code=500, detail="Server misconfiguration")

    token = auth_header.split(" ")[1]

    async with httpx.AsyncClient() as client:
        try:
            res = await client.get(
                f"{SUPABASE_URL}/auth/v1/user",
                headers={
                    "Authorization": f"Bearer {token}",
                    "apikey": SUPABASE_ANON_KEY
                }
            )
        except Exception as e:
            logger.error(f"Supabase request failed in protected router: {e}")
            raise HTTPException(status_code=503, detail="Supabase connection error")

    if res.status_code != 200:
        raise HTTPException(status_code=401, detail="Invalid or expired token")

    return res.json()

@router.get("/agent/protected")
async def protected_route(user_data=Depends(validate_access_token)):
    return {"response": f"Hello {user_data['email']}, this is protected data"}
