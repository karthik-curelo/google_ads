"""/integrations — the connector registry rendered for the UI (§23, §30).

Everything the frontend needs to draw the Integrations screen comes from here:
display metadata, availability (computed from the environment, with the reason
when unavailable), prerequisites, caveats, and the stream catalog.
"""

from __future__ import annotations

from fastapi import APIRouter, HTTPException

from app.api.deps import OrgDep
from app.connectors.registry import availability, load_connectors
from app.core.config import get_settings

router = APIRouter(prefix="/integrations", tags=["integrations"])


@router.get("")
async def list_integrations(_org: OrgDep) -> dict:
    registry = load_connectors()
    settings = get_settings()
    items = []
    for entry in registry.all():
        available, reason = availability(entry, settings)
        items.append(entry.describe(available=available, unavailable_reason=reason))
    return {"integrations": items}


@router.get("/{connector_id}")
async def get_integration(connector_id: str, _org: OrgDep) -> dict:
    registry = load_connectors()
    if connector_id not in registry:
        raise HTTPException(404, f"Unknown connector {connector_id!r}")
    entry = registry.get(connector_id)
    available, reason = availability(entry, get_settings())
    return entry.describe(available=available, unavailable_reason=reason)
