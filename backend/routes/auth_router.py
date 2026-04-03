import logging
from datetime import datetime
from typing import Optional

from fastapi import APIRouter, HTTPException, Request, Depends, Header
from pydantic import BaseModel

from models.user import UserSignupRequest, UserLoginRequest
from backend.services import auth_service
from datastore.supabase_client import supabase
from backend.services.access_guard import (
    get_acl_status,
    is_trusted_dev_execution_context,
    validate_access_token,
)

logger = logging.getLogger(__name__)
router = APIRouter()

# ------------------------------------
# Auth SignUp/Login/Logout Endpoints
# ------------------------------------
@router.post("/auth/signup")
async def signup(user: UserSignupRequest):
    result = auth_service.signup_user(user)
    if "error" in result:
        raise HTTPException(status_code=400, detail=result["error"])
    return result

@router.post("/auth/login")
async def login(user: UserLoginRequest):
    result = auth_service.login_user(user)
    if "error" in result:
        raise HTTPException(status_code=400, detail=result["error"])
    return result


class LogoutPayload(BaseModel):
    session_id: str

@router.post("/auth/logout")
async def logout(
    payload: LogoutPayload,
    user_data=Depends(validate_access_token)
):
    try:
        email = user_data["email"]
        session_id = payload.session_id

        # print("📨 Attempting logout update for:")
        # print("   email      =", email)
        # print("   session_id =", session_id)

        # Optional: Debug check to verify session exists
        debug_check = supabase.table("user_sessions")\
            .select("*")\
            .eq("id", session_id)\
            .execute()
        # print("🔍 Found session with ID:", debug_check.data)

        # Update logout time
        response = supabase.table("user_sessions")\
            .update({"logout_time": datetime.utcnow().isoformat()})\
            .eq("id", session_id)\
            .execute()

        if not response.data or len(response.data) == 0:
            # print("❌ Supabase update failed:", response.error)
            raise HTTPException(status_code=500, detail=f"Supabase update error: {response.error}")

        # print("✅ logout_time updated:", response.data)

        supabase.auth.sign_out()
        return {"message": "Logout successful"}

    except Exception as e:
        # print("❌ Logout failed:", str(e))
        raise HTTPException(status_code=500, detail=f"Logout error: {str(e)}")


# EXAMPLE USAGE in FastAPI router
class UserTrackRequest(BaseModel):
    email: str
    full_name: Optional[str] = None
    provider: Optional[str] = None
    avatar_url: Optional[str] = None

@router.post("/auth/track")
async def track_user(request: Request, user: UserTrackRequest, user_data=Depends(validate_access_token)):
    try:

        # print("📥 Incoming Payload:", user.dict())
        # print("🔐 Decoded Supabase Token User:", user_data)

        # Extract client IP from headers or fallback
        client_host = request.client.host
        forwarded_for = request.headers.get("x-forwarded-for")
        ip_address = forwarded_for.split(",")[0].strip() if forwarded_for else client_host

        result = supabase.table("user_sessions").insert({
            "email": user.email,
            "full_name": user.full_name,
            "provider": user.provider,
            "avatar_url": user.avatar_url,
            "ip_address": ip_address  # ✅ new field
        }).execute()

        if not result.data or len(result.data) == 0:
            raise HTTPException(status_code=500, detail="Supabase insert failed: No data returned.")

        # print("✅ Supabase insert succeeded:", result.data)

        return {
            "message": "User tracked successfully",
            "session_id": result.data[0]["id"]
        }

    except Exception as e:
        # print("❌ ERROR:", str(e))
        raise HTTPException(status_code=500, detail=f"Tracking failed: {str(e)}")


@router.get("/auth/access-mode")
async def access_mode(request: Request, authorization: str | None = Header(default=None)):
    """
    Returns current user's execution mode.
    - whitelist => can_execute=True
    - trusted dev context (loopback / LAN Origin / Cursor dev host) => execute without ACL
    - others    => read-only
    """
    if is_trusted_dev_execution_context(request):
        return {
            "email": "local-dev",
            "acl_status": "local-dev",
            "can_execute": True,
            "mode": "execute",
        }
    user_data = await validate_access_token(authorization)
    email = (user_data.get("email") or "").strip().lower()
    acl_status = get_acl_status(email)
    return {
        "email": email,
        "acl_status": acl_status,
        "can_execute": acl_status == "whitelist",
        "mode": "execute" if acl_status == "whitelist" else "read-only",
    }

