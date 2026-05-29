from __future__ import annotations

from typing import Any, Callable

from fastapi import Depends, FastAPI, HTTPException, Request, Response
from pydantic import BaseModel, Field
from sqlalchemy.orm import Session

from agentic_data_platform.persistence.repositories import IdentityRepository, UserRecord
from agentic_data_platform.service.security import require_authenticated_user
from agentic_data_platform.service.web_sessions import (
    SESSION_COOKIE_NAME,
    create_session_cookie,
    parse_web_login_credentials,
)


class WebLoginRequest(BaseModel):
    username: str = Field(min_length=1)
    password: str = Field(min_length=1)


def register_auth_routes(app: FastAPI, session_dependency: Callable) -> None:
    @app.post("/auth/login", tags=["auth"], responses=_example_response(_LOGIN_EXAMPLE))
    def login(
        payload: WebLoginRequest,
        request: Request,
        response: Response,
        session: Session = Depends(session_dependency),
    ) -> dict[str, Any]:
        settings = request.app.state.settings
        if not settings.web_login_credentials or not settings.web_session_secret:
            raise HTTPException(status_code=503, detail="Web login is not configured")

        credentials = parse_web_login_credentials(settings.web_login_credentials)
        credential = credentials.get(payload.username)
        if credential is None or credential.password != payload.password:
            raise HTTPException(status_code=401, detail="Invalid username or password")

        try:
            user = IdentityRepository(session).get_user(credential.user_id)
        except KeyError as exc:
            raise HTTPException(status_code=401, detail="Configured web user was not found") from exc

        cookie_value, expires_at = create_session_cookie(
            user_id=user.user_id,
            secret=settings.web_session_secret,
            ttl_seconds=settings.web_session_ttl_seconds,
        )
        response.set_cookie(
            SESSION_COOKIE_NAME,
            cookie_value,
            max_age=settings.web_session_ttl_seconds,
            httponly=True,
            secure=settings.environment not in {"dev", "test", "local"},
            samesite="lax",
            path="/",
        )
        return _with_request_id(
            request,
            {
                "user": _user_payload(user),
                "session_expires_at": expires_at.astimezone().isoformat(),
            },
        )

    @app.get("/auth/session", tags=["auth"], responses=_example_response(_SESSION_EXAMPLE))
    def get_session(request: Request, session: Session = Depends(session_dependency)) -> dict[str, Any]:
        auth = require_authenticated_user(request, session)
        return _with_request_id(request, {"user": _user_payload(auth.user)})

    @app.post("/auth/logout", tags=["auth"], responses=_example_response(_LOGOUT_EXAMPLE))
    def logout(request: Request, response: Response) -> dict[str, Any]:
        response.delete_cookie(SESSION_COOKIE_NAME, path="/", samesite="lax")
        return _with_request_id(request, {"logged_out": True})


def _user_payload(user: UserRecord) -> dict[str, str | None]:
    return {
        "user_id": user.user_id,
        "email": user.email,
        "display_name": user.display_name,
        "team_id": user.team_id,
    }


def _with_request_id(request: Request, payload: dict[str, Any]) -> dict[str, Any]:
    request_id = getattr(request.state, "request_id", None)
    if request_id:
        payload["request_id"] = request_id
    return payload


def _example_response(example: dict[str, Any]) -> dict[int, dict[str, Any]]:
    return {200: {"content": {"application/json": {"example": example}}}}


_USER_EXAMPLE = {
    "user_id": "[REDACTED_OWNER]",
    "email": "[REDACTED_OWNER]@example.com",
    "display_name": "[REDACTED_OWNER]",
    "team_id": "pilot-project",
}
_LOGIN_EXAMPLE = {
    "user": _USER_EXAMPLE,
    "session_expires_at": "2026-05-29T20:00:00Z",
    "request_id": "req_123",
}
_SESSION_EXAMPLE = {"user": _USER_EXAMPLE, "request_id": "req_123"}
_LOGOUT_EXAMPLE = {"logged_out": True, "request_id": "req_123"}
