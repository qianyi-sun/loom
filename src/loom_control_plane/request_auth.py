"""Bearer authentication shared by control-plane read and artifact routes."""
from typing import Annotated

from fastapi import Depends, Header, Request

from loom.auth import AuthContext, verify_bearer_token


async def request_principal(
    request: Request, authorization: str | None = Header(default=None),
) -> AuthContext | None:
    async with request.app.state.session_factory() as session:
        return await verify_bearer_token(session, authorization)


RequestPrincipal = Annotated[AuthContext | None, Depends(request_principal)]
