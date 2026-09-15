"""Connector registry (§23) — the single place the platform learns what exists.

Airbyte's equivalent is the connector catalog assembled from each connector's
`metadata.yaml`. Same purpose here: the API, the UI and the sync engine all read
this, so adding a connector means adding a module and one `register()` call
rather than editing a switch statement in four files (§24).

Availability is computed, not stored. A connector whose provider credentials are
absent from the environment is listed as unavailable *with the reason*, because a
silently missing integration is far harder to debug than one that says
"GOOGLE_CLIENT_ID is not set".
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from app.connectors.base import BaseConnector, SyncMode
from app.connectors.errors import invalid_configuration


@dataclass(slots=True)
class RegistryEntry:
    connector_class: type[BaseConnector]
    # Env settings that must be non-empty for this connector to be usable.
    requires_settings: tuple[str, ...] = ()
    # Human-readable prerequisites shown in the UI before the user starts (§8:
    # "Do not hide Google Ads API prerequisites").
    prerequisites: tuple[str, ...] = ()
    # Provider-side gates we cannot satisfy for the user (§9 app review).
    caveats: tuple[str, ...] = ()
    resource_label: str = "Account"
    enabled: bool = True
    tags: tuple[str, ...] = ()

    @property
    def connector_id(self) -> str:
        return self.connector_class.connector_id

    def describe(self, *, available: bool, unavailable_reason: str | None) -> dict[str, Any]:
        cls = self.connector_class
        streams = []
        for stream in cls.declared_streams():
            streams.append(
                {
                    "name": stream.name,
                    "description": stream.description,
                    "grain": stream.grain,
                    "primary_key": stream.primary_key,
                    "supported_sync_modes": [str(m) for m in stream.supported_sync_modes],
                    "default_cursor_field": stream.default_cursor_field,
                    "slice_days": stream.slice_days,
                    "requires": stream.requires,
                }
            )
        return {
            **cls.describe(),
            "resource_label": self.resource_label,
            "prerequisites": list(self.prerequisites),
            "caveats": list(self.caveats),
            "tags": list(self.tags),
            "enabled": self.enabled,
            "available": available,
            "unavailable_reason": unavailable_reason,
            "supported_sync_modes": sorted(
                {str(m) for s in cls.declared_streams() for m in s.supported_sync_modes}
            )
            or [str(SyncMode.INCREMENTAL)],
            "streams": streams,
            "config_schema": cls.config_schema,
        }


class ConnectorRegistry:
    def __init__(self) -> None:
        self._entries: dict[str, RegistryEntry] = {}

    def register(self, entry: RegistryEntry) -> RegistryEntry:
        connector_id = entry.connector_id
        if not connector_id:
            raise ValueError(f"{entry.connector_class.__name__} has no connector_id")
        if connector_id in self._entries:
            raise ValueError(f"Connector {connector_id!r} is already registered")
        self._entries[connector_id] = entry
        return entry

    def get(self, connector_id: str) -> RegistryEntry:
        try:
            return self._entries[connector_id]
        except KeyError:
            raise invalid_configuration(
                f"Unknown connector {connector_id!r}. Known connectors: "
                f"{', '.join(sorted(self._entries)) or 'none'}."
            ) from None

    def connector_class(self, connector_id: str) -> type[BaseConnector]:
        return self.get(connector_id).connector_class

    def all(self) -> list[RegistryEntry]:
        return [self._entries[k] for k in sorted(self._entries)]

    def for_provider(self, provider: str) -> list[RegistryEntry]:
        return [e for e in self.all() if e.connector_class.provider == provider]

    def __contains__(self, connector_id: object) -> bool:
        return connector_id in self._entries

    def __len__(self) -> int:
        return len(self._entries)


registry = ConnectorRegistry()


def availability(entry: RegistryEntry, settings: Any) -> tuple[bool, str | None]:
    """Is this connector usable with the current environment?"""
    if not entry.enabled:
        return False, "This connector is disabled."
    missing = [
        name.upper() for name in entry.requires_settings if not str(getattr(settings, name, "") or "").strip()
    ]
    if missing:
        return False, f"Not configured — set {', '.join(missing)} in the environment."
    return True, None


_loaded = False


def load_connectors() -> ConnectorRegistry:
    """Import every connector module so its `register()` runs.

    Explicit rather than directory-scanning: an import error in a connector should
    fail loudly at startup, not silently drop the integration from the UI. Guarded
    by a flag (not `len(registry)`) so that importing one connector module
    directly — as tests do — does not stop the rest from loading.
    """
    global _loaded
    if _loaded:
        return registry
    from app.connectors.google import ads as _ads  # noqa: F401
    from app.connectors.google import analytics as _ga4  # noqa: F401
    from app.connectors.google import search_console as _gsc  # noqa: F401
    from app.connectors.leadsquared import connector as _leadsquared  # noqa: F401
    from app.connectors.meta import ads as _meta_ads  # noqa: F401
    from app.connectors.meta import instagram as _instagram  # noqa: F401
    from app.connectors.meta import pages as _fb_pages  # noqa: F401

    _loaded = True
    return registry


__all__ = ["ConnectorRegistry", "RegistryEntry", "availability", "load_connectors", "registry"]
