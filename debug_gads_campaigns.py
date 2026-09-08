"""
Diagnose the Google Ads 'campaigns' stream failure for CURIS HEALTHTECH.
Tests the exact GAQL query used by the connector against the live API.

The campaigns stream uses:
  SELECT campaign.id, campaign.name, campaign.status,
         campaign.advertising_channel_type,
         campaign.start_date, campaign.end_date,
         campaign_budget.amount_micros
  FROM campaign

Error: "The provider rejected the request as invalid" (INVALID_ARGUMENT)
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

CUSTOMER_ID = "9232673741"  # CURIS HEALTHTECH PRIVATE LIMITED


async def get_google_token() -> str:
    """Get the Google OAuth token for the google_ads connection."""
    async with SessionLocal() as session:
        # Find the google_ads connection
        result = await session.execute(
            sa_select(Connection).where(Connection.connector_id == "google_ads")
        )
        conn = result.scalars().first()
        if not conn:
            print("ERROR: No google_ads connection found in DB")
            sys.exit(1)

        print(f"Found connection: ID={conn.id}, resource_id={conn.resource_id}, identity_id={conn.oauth_identity_id}")

        identity = await session.get(OAuthIdentity, conn.oauth_identity_id)
        token = decrypt_optional(identity.access_token_encrypted) or ""
        expires = identity.access_token_expires_at
        print(f"Identity: {identity.email or identity.display_name}, expires={expires}")
        print(f"Token[:40]: {token[:40]}...")
        return token


async def run_query(client, customer_id: str, query: str, token: str, label: str) -> dict:
    headers = {
        "Authorization": f"Bearer {token}",
        "developer-token": GOOGLE_ADS_DEVELOPER_TOKEN,
        "Content-Type": "application/json",
    }
    if GOOGLE_ADS_LOGIN_CUSTOMER_ID:
        headers["login-customer-id"] = GOOGLE_ADS_LOGIN_CUSTOMER_ID.replace("-", "")

    print(f"\n{'='*60}")
    print(f"  QUERY: {label}")
    print(f"  Customer: {customer_id}")
    print(f"  GAQL: {query[:100]}...")
    print('='*60)

    r = await client.post(
        f"{BASE}/customers/{customer_id}/googleAds:searchStream",
        headers=headers,
        json={"query": query},
        timeout=30,
    )
    body = r.json()
    if r.status_code != 200:
        print(f"  STATUS: {r.status_code} ERROR")
        print(json.dumps(body, indent=2))
    else:
        batches = body if isinstance(body, list) else [body]
        rows = []
        for batch in batches:
            rows.extend(batch.get("results", []))
        print(f"  STATUS: {r.status_code} OK — {len(rows)} rows returned")
        if rows:
            print(f"  First row: {json.dumps(rows[0], indent=2)[:300]}")
    return body


async def main():
    token = await get_google_token()

    async with httpx.AsyncClient(timeout=30) as client:

        # 1. Test the EXACT failing query from the connector
        await run_query(client, CUSTOMER_ID, """
SELECT campaign.id, campaign.name, campaign.status,
       campaign.advertising_channel_type,
       campaign.start_date, campaign.end_date,
       campaign_budget.amount_micros
FROM campaign
""", token, "EXACT campaigns query from connector")

        # 2. Try without campaign_budget (campaign_budget is a joined resource — might need WHERE)
        await run_query(client, CUSTOMER_ID, """
SELECT campaign.id, campaign.name, campaign.status,
       campaign.advertising_channel_type,
       campaign.start_date, campaign.end_date
FROM campaign
""", token, "campaigns WITHOUT campaign_budget.amount_micros")

        # 3. campaign_budget requires joining via campaign.campaign_budget
        await run_query(client, CUSTOMER_ID, """
SELECT campaign.id, campaign.name, campaign.status,
       campaign.advertising_channel_type,
       campaign.start_date, campaign.end_date,
       campaign.campaign_budget
FROM campaign
""", token, "campaigns with campaign.campaign_budget (resource name, not micros)")

        # 4. The correct way — campaign_budget must be attributed to campaign resource
        await run_query(client, CUSTOMER_ID, """
SELECT campaign.id, campaign.name, campaign.status,
       campaign.advertising_channel_type,
       campaign.start_date, campaign.end_date
FROM campaign
LIMIT 5
""", token, "campaigns minimal — verify basic access")

        # 5. Check if campaign_budget.amount_micros IS valid from campaign_budget resource
        await run_query(client, CUSTOMER_ID, """
SELECT campaign.id, campaign.name, campaign_budget.amount_micros
FROM campaign_budget
LIMIT 5
""", token, "campaign_budget FROM campaign_budget resource (correct attribution)")

        print(f"\n{'='*60}")
        print("  DIAGNOSIS SUMMARY")
        print('='*60)
        print("  If query 1 failed but query 2 succeeded:")
        print("  -> campaign_budget.amount_micros cannot be selected FROM campaign")
        print("     It must be selected FROM campaign_budget resource.")
        print("  Fix: Change spec['resource'] to 'campaign_budget' OR remove campaign_budget.amount_micros")
        print("       from the campaigns SELECT and use campaign.campaign_budget instead.")


if __name__ == "__main__":
    asyncio.run(main())
