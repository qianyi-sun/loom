from __future__ import annotations

from dataclasses import dataclass
from http import HTTPStatus
from uuid import uuid4

from fastapi import HTTPException, Request
from fastapi.responses import JSONResponse
from starlette.middleware.base import BaseHTTPMiddleware

from agentic_data_platform.persistence.repositories import IdentityRepository, ProjectRecord, ProjectRepository, UserRecord
from agentic_data_platform.service.web_sessions import SESSION_COOKIE_NAME, SessionCookieError, verify_session_cookie


AUTHENTICATED_USER_STATE_KEY = "authenticated_user_id"
AUTH_EXEMPT_PATHS = {"/", "/healthz", "/readyz", "/docs", "/redoc", "/openapi.json", "/auth/login"}
AUTH_EXEMPT_PREFIXES = ("/docs/", "/app/")
ROLE_LEVELS = {"viewer": 1, "member": 2, "owner": 3, "admin": 3}


@dataclass(frozen=True)
class AuthContext:
    user: UserRecord


class InternalAuthMiddleware(BaseHTTPMiddleware):
    def __init__(self, app, *, internal_auth_tokens: str, web_session_secret: str = "") -> None:
        super().__init__(app)
        self._token_to_user_id = parse_internal_auth_tokens(internal_auth_tokens)
        self._web_session_secret = web_session_secret

    async def dispatch(self, request: Request, call_next):
        if _is_exempt_path(request.url.path):
            return await call_next(request)

        request_id = _request_id(request)
        token = _bearer_token(request.headers.get("Authorization", ""))
        if token is not None:
            user_id = self._token_to_user_id.get(token)
            if user_id is None:
                return _auth_error(request_id=request_id, status_code=401, message="Bearer token is invalid")
            request.state.authenticated_user_id = user_id
            return await call_next(request)

        cookie_value = request.cookies.get(SESSION_COOKIE_NAME)
        if cookie_value and self._web_session_secret:
            try:
                request.state.authenticated_user_id = verify_session_cookie(
                    cookie_value,
                    secret=self._web_session_secret,
                )
            except SessionCookieError:
                return _auth_error(request_id=request_id, status_code=401, message="Web session is invalid")
            return await call_next(request)

        if cookie_value:
            return _auth_error(request_id=request_id, status_code=401, message="Web session is not configured")
        return _auth_error(request_id=request_id, status_code=401, message="Bearer token or web session is required")


def parse_internal_auth_tokens(raw_value: str) -> dict[str, str]:
    token_to_user_id: dict[str, str] = {}
    for item in raw_value.split(","):
        item = item.strip()
        if not item:
            continue
        if "=" not in item:
            raise ValueError("INTERNAL_AUTH_TOKENS entries must use user_id=token")
        user_id, token = [part.strip() for part in item.split("=", 1)]
        if not user_id or not token:
            raise ValueError("INTERNAL_AUTH_TOKENS entries must include both user_id and token")
        token_to_user_id[token] = user_id
    return token_to_user_id


def require_authenticated_user(request: Request, session) -> AuthContext:
    user_id = getattr(request.state, AUTHENTICATED_USER_STATE_KEY, None)
    if not user_id:
        raise HTTPException(status_code=401, detail="Bearer token is required")
    try:
        user = IdentityRepository(session).get_user(user_id)
    except KeyError as exc:
        raise HTTPException(status_code=401, detail=f"Authenticated user not found: {user_id}") from exc
    return AuthContext(user=user)


def require_same_actor(auth: AuthContext, actor_user_id: str | None) -> str:
    if actor_user_id is not None and actor_user_id != auth.user.user_id:
        raise HTTPException(status_code=403, detail="Authenticated user cannot act as another user")
    return auth.user.user_id


def require_project_role(session, auth: AuthContext, project_id: str, *, minimum_role: str) -> ProjectRecord:
    try:
        project = ProjectRepository(session).get_project(project_id)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail="Project not found") from exc

    role = IdentityRepository(session).get_membership_role(
        team_id=project.owner_team_id,
        user_id=auth.user.user_id,
    )
    if _role_level(role) < _role_level(minimum_role):
        raise HTTPException(status_code=403, detail="User is not authorized for this project")
    return project


def accessible_project_ids(session, auth: AuthContext) -> set[str]:
    return set(ProjectRepository(session).list_project_ids_for_user(auth.user.user_id))


def _role_level(role: str | None) -> int:
    return ROLE_LEVELS.get(role or "", 0)


def _is_exempt_path(path: str) -> bool:
    return path in AUTH_EXEMPT_PATHS or any(path.startswith(prefix) for prefix in AUTH_EXEMPT_PREFIXES)


def _bearer_token(header_value: str) -> str | None:
    parts = header_value.split(" ", 1)
    if len(parts) != 2 or parts[0].lower() != "bearer":
        return None
    token = parts[1].strip()
    return token or None


def _auth_error(*, request_id: str, status_code: int, message: str) -> JSONResponse:
    return JSONResponse(
        status_code=status_code,
        content={
            "error": {
                "code": _http_error_code(status_code),
                "message": message,
                "request_id": request_id,
            }
        },
        headers={"X-Request-ID": request_id, "WWW-Authenticate": "Bearer"},
    )


def _request_id(request: Request) -> str:
    request_id = getattr(request.state, "request_id", None) or request.headers.get("X-Request-ID") or str(uuid4())
    request.state.request_id = request_id
    return request_id


def _http_error_code(status_code: int) -> str:
    try:
        return HTTPStatus(status_code).phrase.lower().replace(" ", "_")
    except ValueError:
        return "http_error"
