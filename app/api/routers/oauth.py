"""OAuth connect / callback / identity management (§5, §22, §27, §30).

Flow: POST .../connect returns a provider authorization URL → the user consents
→ the provider redirects to GET /oauth/{provider}/callback → we exchange the
code, resolve the identity, store encrypted tokens, and bounce back to the app.

The callback is intentionally unauthenticated (the provider calls it) — CSRF is
covered by the single-use `state` row created at connect time (§5).
"""

from __future__ import annotations

from fastapi import APIRouter, HTTPException, Query
from fastapi.responses import HTMLResponse, RedirectResponse
from pydantic import BaseModel
from sqlalchemy import select

from app.api.deps import OrgDep, SessionDep
from app.connectors import errors as E
from app.connectors.registry import load_connectors
from app.core.config import get_settings
from app.models import OAuthIdentity
from app.oauth.service import (
    begin_authorization,
    complete_authorization,
    disconnect_identity,
)

router = APIRouter(tags=["oauth"])


class ConnectRequest(BaseModel):
    redirect_after: str | None = None
    identity_id: int | None = None  # set to re-authorise an existing identity
    login_hint: str | None = None


class ConnectResponse(BaseModel):
    authorization_url: str
    state: str


@router.post("/integrations/{connector_id}/connect", response_model=ConnectResponse)
async def connect(connector_id: str, body: ConnectRequest, org: OrgDep, session: SessionDep):
    registry = load_connectors()
    if connector_id not in registry:
        raise HTTPException(404, f"Unknown connector {connector_id!r}")
    try:
        url, state = await begin_authorization(
            session,
            organization_id=org.id,
            connector_id=connector_id,
            redirect_after=body.redirect_after,
            identity_id=body.identity_id,
            login_hint=body.login_hint,
        )
    except E.ConnectorError as exc:
        raise HTTPException(400, exc.as_user_dict()) from exc
    return ConnectResponse(authorization_url=url, state=state)


@router.get("/oauth/{provider}/callback")
async def oauth_callback(
    provider: str,
    session: SessionDep,
    code: str | None = Query(default=None),
    state: str | None = Query(default=None),
    error: str | None = Query(default=None),
    error_description: str | None = Query(default=None),
):
    if error:
        return _result_page(
            False, f"{provider.title()} authorisation was denied: {error_description or error}"
        )
    if not code or not state:
        return _result_page(False, "The provider callback was missing its code or state.")

    try:
        identity, state_row = await complete_authorization(session, state=state, code=code)
    except E.ConnectorError as exc:
        return _result_page(False, exc.message)

    if state_row.redirect_after:
        sep = "&" if "?" in state_row.redirect_after else "?"
        target = (
            f"{state_row.redirect_after}{sep}status=connected"
            f"&identity_id={identity.id}&connector_id={state_row.connector_id or ''}"
        )
        return RedirectResponse(target, status_code=302)

    return _result_page(
        True,
        f"Connected as {identity.email or identity.display_name or identity.external_account_id}. "
        "You can close this window and return to the app.",
    )


class IdentityOut(BaseModel):
    id: int
    provider: str
    email: str | None
    display_name: str | None
    status: str
    status_detail: str | None
    scopes: list[str]


@router.get("/identities", response_model=list[IdentityOut])
async def list_identities(org: OrgDep, session: SessionDep):
    rows = (
        (
            await session.execute(
                select(OAuthIdentity)
                .where(OAuthIdentity.organization_id == org.id)
                .order_by(OAuthIdentity.created_at.desc())
            )
        )
        .scalars()
        .all()
    )
    return [
        IdentityOut(
            id=r.id,
            provider=r.provider,
            email=r.email,
            display_name=r.display_name,
            status=r.status,
            status_detail=r.status_detail,
            scopes=list(r.scopes or []),
        )
        for r in rows
    ]


@router.delete("/identities/{identity_id}")
async def delete_identity(identity_id: int, org: OrgDep, session: SessionDep):
    identity = await session.get(OAuthIdentity, identity_id)
    if identity is None or identity.organization_id != org.id:
        raise HTTPException(404, "Identity not found")
    revoked = await disconnect_identity(session, identity)
    return {"disconnected": True, "provider_revocation_acknowledged": revoked}


def _result_page(ok: bool, message: str) -> HTMLResponse:
    settings = get_settings()
    colour = "#0a7d33" if ok else "#b00020"
    title = "Connected" if ok else "Connection failed"
    back = settings.frontend_base_url
    html = f"""<!doctype html><html><head><meta charset="utf-8">
<title>{title}</title><style>
body{{font:16px/1.5 system-ui,sans-serif;max-width:32rem;margin:4rem auto;padding:0 1rem;color:#111}}
.badge{{color:{colour};font-weight:600}}a{{color:#0a58ca}}</style></head>
<body><p class="badge">{title}</p><p>{message}</p>
<p><a href="{back}">Return to the app</a></p></body></html>"""
    return HTMLResponse(html, status_code=200 if ok else 400)
