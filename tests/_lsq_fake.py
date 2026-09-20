"""An in-memory LeadSquared, faithful to the behaviours that were verified live.

Wired into `respx` so the REAL connector, HTTP client, window fetcher, writer and
runner run against it. What it reproduces (each was observed on api-in21):

* `Leads.RecentlyModified` filters on `LeadLastModifiedOn`; `RetrieveByActivityEvent`
  filters on `ModifiedOn` — both inclusive, at second resolution;
* `RecordCount` is the true total for the window whatever the page size, and the
  list key is omitted when there are no rows;
* page caps (5000 leads / 1000 activities) answer HTTP 500 + MXInvalidInputException,
  as does a missing ActivityEvent — LeadSquared reports logical errors as 500;
* stored timestamps carry SUB-SECOND precision while the API prints `.000` and parses From/ToDate
  as whole seconds (`ToDate=12:12:04` means "up to 12:12:04.000"). Two adjacent windows therefore
  miss the rows in the crack between them — the defect found live on 2026-09-20 (`subsecond=True`);
* ordering by a NON-unique column (CreatedOn/ModifiedOn) is unstable across requests
  when timestamps tie (`unstable_ties=True`): the source of the real row loss;
* ordering by the unique id is stable.

Failure injection (`inject`) and a pre-request hook (`before_request`) let a test
throw 429s / timeouts / malformed bodies or mutate the data mid-pull.
"""

from __future__ import annotations

import json
import random
from collections.abc import Callable
from datetime import datetime, timedelta
from typing import Any

import httpx

FMT = "%Y-%m-%d %H:%M:%S"
HOST = "https://lsq.test"


def ts(value: datetime | str) -> str:
    return value if isinstance(value, str) else value.strftime(FMT)


class FakeLeadSquared:
    def __init__(self, *, unstable_ties: bool = True, seed: int = 7, subsecond: bool = True) -> None:
        self.subsecond = subsecond
        self._ms: dict[tuple[str, str], int] = {}  # (kind, id) -> hidden milliseconds
        self.leads: dict[str, dict[str, Any]] = {}
        self.activities: dict[int, dict[str, dict[str, Any]]] = {}
        self.activity_types: dict[int, str] = {}  # served by ActivityTypes.Get
        self.unstable_ties = unstable_ties
        self.rng = random.Random(seed)
        self.requests: list[dict[str, Any]] = []  # every retrieval request body, in order
        self.inject: list[Callable[[dict], httpx.Response | Exception | None]] = []
        self.before_request: Callable[[int, str, dict], None] | None = None
        self.lead_field_count = 206
        self.leaf_responses = 0  # responses that held a whole window (<= one page, non-empty)
        self.metadata_ok = True

    # ------------------------------------------------------------------ data
    def add_lead(
        self,
        prospect_id: str,
        last_modified: datetime | str,
        *,
        created: datetime | str | None = None,
        **attrs: Any,
    ) -> dict[str, Any]:
        row = {
            "ProspectID": prospect_id,
            "CreatedOn": ts(created or last_modified),
            "ModifiedOn": ts(last_modified),
            "LeadLastModifiedOn": ts(last_modified),
            **attrs,
        }
        self.leads[prospect_id] = row
        self._ms[("lead", prospect_id)] = self._new_ms()
        return row

    def add_activity(
        self,
        code: int,
        activity_id: str,
        modified: datetime | str,
        *,
        created: datetime | str | None = None,
        **fields: Any,
    ) -> dict[str, Any]:
        row = {
            "ProspectActivityId": activity_id,
            "RelatedProspectId": fields.pop("RelatedProspectId", "p1"),
            "ActivityEvent": str(code),
            "ActivityEvent_Note": fields.pop("ActivityEvent_Note", f"type {code}"),
            "Status": "Active",
            "CreatedOn": ts(created or modified),
            "ModifiedOn": ts(modified),
            **fields,
        }
        self.activities.setdefault(code, {})[activity_id] = row
        self._ms[("act", activity_id)] = self._new_ms()
        return row

    def _new_ms(self) -> int:
        return self.rng.randrange(1000) if self.subsecond else 0

    def _real(self, kind: str, id_col: str, filter_col: str, row: dict) -> datetime:
        """The row's true timestamp: the printed seconds plus its hidden milliseconds."""
        return datetime.strptime(row[filter_col], FMT) + timedelta(
            milliseconds=self._ms.get((kind, row[id_col]), 0)
        )

    def delete_activity(self, code: int, activity_id: str) -> None:
        self.activities.get(code, {}).pop(activity_id, None)

    def calls_to(self, needle: str) -> list[dict[str, Any]]:
        return [r for r in self.requests if needle in r["path"]]

    # -------------------------------------------------------------- handler
    def handle(self, request: httpx.Request) -> httpx.Response:
        path = request.url.path
        body: dict[str, Any] = json.loads(request.content) if request.content else {}
        for hook in list(self.inject):
            injected = hook({"path": path, "body": body})
            if injected is not None:
                if isinstance(injected, Exception):
                    raise injected
                return injected
        if "Leads.RecentlyModified" in path or "RetrieveByActivityEvent" in path:
            self.requests.append({"path": path, "body": body})
            if self.before_request is not None:
                self.before_request(len(self.requests), path, body)
        if path.endswith("LeadsMetaData.Get"):
            if not self.metadata_ok:
                return _mx(401, "MXInvalidAccessDetailsException", "Invalid Access Details provided.")
            return httpx.Response(200, json=[{"SchemaName": f"f{i}"} for i in range(self.lead_field_count)])
        if path.endswith("GetActivityDetails"):
            wanted = request.url.params.get("activityId")
            for rows in self.activities.values():
                if wanted in rows:
                    return httpx.Response(200, json={"ID": wanted, "Fields": []})
            return _mx(500, "MXUnknownProspectActivityException", "Activity does not exist.")
        if path.endswith("Leads.GetById"):
            found = self.leads.get(request.url.params.get("id"))
            # live behaviour: a one-element list for a real lead, an EMPTY list otherwise
            return httpx.Response(200, json=[{"LeadPropertyList": []}] if found else [])
        if path.endswith("ActivityTypes.Get"):
            return httpx.Response(
                200,
                json=[{"ActivityEvent": c, "ActivityEventName": n} for c, n in self.activity_types.items()],
            )
        if path.endswith("Leads.RecentlyModified"):
            return self._retrieve(body, kind="lead")
        if path.endswith("RetrieveByActivityEvent"):
            return self._retrieve(body, kind="activity")
        return httpx.Response(404, json={})

    def _retrieve(self, body: dict[str, Any], *, kind: str) -> httpx.Response:
        param = body.get("Parameter")
        if not param:
            return _mx(
                500,
                "MXInvalidInputException",
                "Invalid Input! Parameter Name: You have not passed Parameter.",
            )
        paging = body.get("Paging") or {}
        page_size = int(paging.get("PageSize", 25))
        page_index = int(paging.get("PageIndex", 1))
        cap = 5000 if kind == "lead" else 1000
        if page_size > cap:
            msg = f"PageSize can not be more than {cap}."
            return _mx(500, "MXInvalidInputException", f"Invalid Input! Parameter Name: {msg}")
        try:
            frm = datetime.strptime(param["FromDate"], FMT)
            to = datetime.strptime(param["ToDate"], FMT)
        except (KeyError, ValueError):
            return _mx(
                500,
                "MXInvalidInputException",
                "Invalid Input! Parameter Name: FromDate value is not correct.",
            )
        if frm > to:
            return _mx(
                500,
                "MXInvalidInputException",
                "Invalid Input! Parameter Name: FromDate can't be greater than ToDate.",
            )

        if kind == "lead":
            pool = list(self.leads.values())
            filter_col, id_col, key = "LeadLastModifiedOn", "ProspectID", "Leads"
        else:
            if "ActivityEvent" not in param:
                return _mx(
                    500,
                    "MXInvalidInputException",
                    "Invalid Input! Parameter Name: You have not passed any ActivityEvent.",
                )
            pool = list(self.activities.get(int(param["ActivityEvent"]), {}).values())
            filter_col, id_col, key = "ModifiedOn", "ProspectActivityId", "List"

        kind_key = "lead" if kind == "lead" else "act"
        hits = [r for r in pool if frm <= self._real(kind_key, id_col, filter_col, r) <= to]
        sort_col = (body.get("Sorting") or {}).get("ColumnName") or "CreatedOn"
        rows = self._order(hits, sort_col, id_col)
        total = len(rows)
        if 0 < total <= page_size and page_index == 1:
            self.leaf_responses += 1
        start = (page_index - 1) * page_size
        page = rows[start : start + page_size] if page_index >= 1 else []
        if total == 0 or not page and page_index > 1:
            return httpx.Response(200, json={"RecordCount": 0})
        if kind == "lead":
            cols = ((body.get("Columns") or {}).get("Include_CSV") or "").split(",")
            cols = [c for c in cols if c]
            payload_rows = [
                {
                    "LeadPropertyList": [
                        {"Attribute": k, "Value": v} for k, v in r.items() if not cols or k in cols
                    ]
                    # live: every lead in a response also carries the QUERY's total,
                    # which differs from one read of the same lead to the next
                    + [{"Attribute": "Total", "Value": str(total)}]
                }
                for r in page
            ]
        else:
            payload_rows = page
        return httpx.Response(200, json={"RecordCount": total, key: payload_rows})

    def _order(self, rows: list[dict], sort_col: str, id_col: str) -> list[dict]:
        if sort_col == id_col:
            return sorted(rows, key=lambda r: r[id_col])
        # A non-unique sort key: ties come back in an order that is NOT stable
        # between requests (what LeadSquared really does).
        if self.unstable_ties:
            shuffled = rows[:]
            self.rng.shuffle(shuffled)
        else:
            shuffled = sorted(rows, key=lambda r: r[id_col])
        return sorted(shuffled, key=lambda r: r.get(sort_col) or "")


def _mx(status: int, exc_type: str, message: str) -> httpx.Response:
    return httpx.Response(
        status, json={"Status": "Error", "ExceptionType": exc_type, "ExceptionMessage": message}
    )


def install(router, fake: FakeLeadSquared) -> None:
    """Route every request to `fake` on a respx router."""
    router.route(host="lsq.test").mock(side_effect=fake.handle)
