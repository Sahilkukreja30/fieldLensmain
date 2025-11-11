# app/routes/auth.py
import os
import time
from typing import Optional

from fastapi import APIRouter, HTTPException, Response, Request
from jose import jwt, JWTError

router = APIRouter()

# --- Config ---
JWT_SECRET = os.getenv("ADMIN_JWT_SECRET", "change-me")
JWT_AUDIENCE = "fieldlens-admin"
JWT_ISSUER = "fieldlens-api"

SESSION_COOKIE = "fl_admin"
SESSION_TTL =60 * 60 * 8  # default: 8h

# When served over HTTPS (HF/Vercel), keep this True.
SECURE_COOKIE = os.getenv("COOKIE_SECURE", "true").lower() == "true"
# If your API lives at https://telecom02-telecom.hf.space, you can leave this empty.
# If you use a custom domain/subdomain for the API, set COOKIE_DOMAIN to that host.
COOKIE_DOMAIN = os.getenv("COOKIE_DOMAIN", "").strip() or None
# SameSite=None is required for cross-site credentialed requests from your Vercel UI.
COOKIE_SAMESITE = "none"
COOKIE_PATH = "/"

# --- Admin creds (env) ---
ADMIN_USER = os.getenv("ADMIN_USERNAME", "admin")
ADMIN_PASS = os.getenv("ADMIN_PASSWORD", "admin123")  # change in env!

# --- Helpers ---
def _make_jwt(sub: str) -> str:
    now = int(time.time())
    payload = {
        "sub": sub,
        "iat": now,
        "exp": now + SESSION_TTL,
        "aud": JWT_AUDIENCE,
        "iss": JWT_ISSUER,
    }
    return jwt.encode(payload, JWT_SECRET, algorithm="HS256")

def _verify(req: Request) -> Optional[str]:
    tok = req.cookies.get(SESSION_COOKIE)
    if not tok:
        return None
    try:
        data = jwt.decode(
            tok,
            JWT_SECRET,
            algorithms=["HS256"],
            audience=JWT_AUDIENCE,
            issuer=JWT_ISSUER,
        )
        return str(data.get("sub"))
    except JWTError:
        return None

def _set_session_cookie(response: Response, token: str) -> None:
    response.set_cookie(
        key=SESSION_COOKIE,
        value=token,
        max_age=SESSION_TTL,
        httponly=True,
        secure=SECURE_COOKIE,
        samesite=COOKIE_SAMESITE,
        path=COOKIE_PATH,
        domain=COOKIE_DOMAIN,  # None = omit
    )

def _clear_session_cookie(response: Response) -> None:
    """
    Clear robustly:
      1) delete_cookie() with the same attributes
      2) overwrite the cookie with an immediate expiry
    """
    response.delete_cookie(
        key=SESSION_COOKIE,
        path=COOKIE_PATH,
        domain=COOKIE_DOMAIN,
    )
    response.set_cookie(
        key=SESSION_COOKIE,
        value="",
        max_age=0,
        expires=0,
        httponly=True,
        secure=SECURE_COOKIE,
        samesite=COOKIE_SAMESITE,
        path=COOKIE_PATH,
        domain=COOKIE_DOMAIN,
    )

# --- Routes ---
@router.post("/auth/login")
def login(payload: dict, response: Response):
    if payload.get("username") != ADMIN_USER or payload.get("password") != ADMIN_PASS:
        raise HTTPException(status_code=401, detail="Invalid credentials")
    token = _make_jwt(ADMIN_USER)
    _set_session_cookie(response, token)
    return {"ok": True}

@router.get("/auth/me")
def me(req: Request):
    sub = _verify(req)
    if not sub:
        raise HTTPException(status_code=401, detail="Not authenticated")
    return {"user": {"username": sub}}

@router.post("/auth/logout")
def logout(response: Response):
    _clear_session_cookie(response)
    return {"ok": True}
