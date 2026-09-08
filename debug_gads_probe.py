"""
Try all plausible field names for campaign dates in Google Ads API v25.
"""
import asyncio
import os
import sys
import json

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from dotenv import load_dotenv
load_dotenv()

import httpx
from app.core.database import SessionLocal
from app.core.crypto import decrypt_optional
from app.models import OAuthIdentity, Connection
from sqlalchemy import select as sa_select

GOOGLE_ADS_DEVELOPER_TOKEN = os.environ.get("GOOGLE_ADS_DEVELOPER_TOKEN", "")
GOOGLE_ADS_LOGIN_CUSTOMER_ID = os.environ.get("GOOGLE_ADS_LOGIN_CUSTOMER_ID", "")
GOOGLE_ADS_API_VERSION = os.environ.get("GOOGLE_ADS_API_VERSION", "v25")
BASE = f"https://googleads.googleapis.com/{GOOGLE_ADS_API_VERSION}"
CUSTOMER_ID = "9232673741"


async def get_token() -> str:
    async with SessionLocal() as session:
        result = await session.execute(sa_select(Connection).where(Connection.connector_id == "google_ads"))
        conn = result.scalars().first()
        identity = await session.get(OAuthIdentity, conn.oauth_identity_id)
        return decrypt_optional(identity.access_token_encrypted) or ""


async def try_field(client, field: str, token: str) -> tuple[bool, str]:
    headers = {
        "Authorization": f"Bearer {token}",
        "developer-token": GOOGLE_ADS_DEVELOPER_TOKEN,
        "Content-Type": "application/json",
    }
    if GOOGLE_ADS_LOGIN_CUSTOMER_ID:
        headers["login-customer-id"] = GOOGLE_ADS_LOGIN_CUSTOMER_ID.replace("-", "")
    r = await client.post(
        f"{BASE}/customers/{CUSTOMER_ID}/googleAds:searchStream",
        headers=headers,
        json={"query": f"SELECT campaign.id, {field} FROM campaign LIMIT 1"},
        timeout=20,
    )
    body = r.json()
    if r.status_code == 200:
        batches = body if isinstance(body, list) else [body]
        rows = [row for batch in batches for row in batch.get("results", [])]
        val = str(rows[0]) if rows else "(no rows)"
        return True, val[:120]
    errs = body[0].get("error", {}).get("details", [{}])[0].get("errors", [{}]) if isinstance(body, list) else body.get("error", {}).get("details", [{}])[0].get("errors", [{}])
    msg = errs[0].get("message", "unknown error") if errs else "unknown"
    return False, msg


async def main():
    token = await get_token()

    # All possible campaign date field name variants to try
    date_field_candidates = [
        "campaign.start_date",
        "campaign.end_date",
        "campaign.campaign_start_date",
        "campaign.campaign_end_date",
        "campaign.start_date_time",
        "campaign.end_date_time",
        "campaign.base_campaign",
        "campaign.bidding_strategy_type",
        "campaign.serving_status",
        "campaign.campaign_group",
    ]

    async with httpx.AsyncClient() as client:
        print("\n" + "="*70)
        print("  PROBING ALL PLAUSIBLE CAMPAIGN DATE FIELDS (API v25)")
        print("="*70)
        for field in date_field_candidates:
            ok, detail = await try_field(client, field, token)
            status = "OK  " if ok else "FAIL"
            print(f"  [{status}] {field}")
            if ok:
                print(f"         -> {detail}")
            else:
                print(f"         -> {detail[:80]}")

        # Get the full campaign resource schema by fetching campaign with just id
        print("\n" + "="*70)
        print("  FULL CAMPAIGN ROW (all available fields from a working query)")
        print("="*70)
        r = await client.post(
            f"{BASE}/customers/{CUSTOMER_ID}/googleAds:searchStream",
            headers={
                "Authorization": f"Bearer {token}",
                "developer-token": GOOGLE_ADS_DEVELOPER_TOKEN,
                "Content-Type": "application/json",
                **({"login-customer-id": GOOGLE_ADS_LOGIN_CUSTOMER_ID.replace("-", "")} if GOOGLE_ADS_LOGIN_CUSTOMER_ID else {}),
            },
            json={"query": "SELECT campaign.id, campaign.name, campaign.status, campaign.advertising_channel_type, campaign_budget.amount_micros FROM campaign LIMIT 1"},
            timeout=20,
        )
        body = r.json()
        batches = body if isinstance(body, list) else [body]
        rows = [row for batch in batches for row in batch.get("results", [])]
        if rows:
            print(json.dumps(rows[0], indent=2))
            print("\n  Available top-level keys in response:")
            for k, v in rows[0].items():
                print(f"    {k}: {json.dumps(v)[:80]}")


if __name__ == "__main__":
    asyncio.run(main())
