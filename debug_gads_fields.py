"""
Find the correct Google Ads field names for campaign start/end date in API v25.
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


async def query(client, gaql: str, token: str, label: str) -> list:
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
        json={"query": gaql},
        timeout=30,
    )
    body = r.json()
    ok = r.status_code == 200
    batches = body if isinstance(body, list) else [body]
    rows = []
    if ok:
        for batch in batches:
            rows.extend(batch.get("results", []))
    err = None if ok else body
    return rows, err


async def main():
    token = await get_token()

    async with httpx.AsyncClient() as client:
        candidates = [
            ("campaign.start_date", "SELECT campaign.id, campaign.name, campaign.start_date FROM campaign LIMIT 1"),
            ("campaign.end_date", "SELECT campaign.id, campaign.name, campaign.end_date FROM campaign LIMIT 1"),
            ("campaign.campaign_start_date", "SELECT campaign.id, campaign.name, campaign.campaign_start_date FROM campaign LIMIT 1"),
            ("segments.date only", "SELECT campaign.id, campaign.name FROM campaign LIMIT 3"),
            ("campaign_budget.amount_micros FROM campaign", "SELECT campaign.id, campaign_budget.amount_micros FROM campaign LIMIT 1"),
            ("campaign_budget.amount_micros FROM campaign_budget", "SELECT campaign.id, campaign.name, campaign_budget.amount_micros FROM campaign_budget LIMIT 1"),
        ]

        print("\n" + "="*70)
        print("  TESTING FIELD NAMES IN GOOGLE ADS API v25")
        print("="*70)
        for label, gaql in candidates:
            rows, err = await query(client, gaql, token, label)
            status = "OK" if err is None else "FAIL"
            detail = ""
            if err:
                errs = err[0].get("error", {}).get("details", [{}])[0].get("errors", [{}])
                detail = errs[0].get("message", "") if errs else ""
            print(f"  [{status}] {label}")
            if detail:
                print(f"         -> {detail}")
            elif rows:
                print(f"         -> {json.dumps(rows[0], indent=None)[:120]}")


if __name__ == "__main__":
    asyncio.run(main())
