"""
Check the SPECIFIC ad account used by the platform connection (act_1226814221486092)
using the token ACTUALLY stored in the database (identity 3 = Karthik K C).
"""
import asyncio
import hashlib
import hmac
import json
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from dotenv import load_dotenv
load_dotenv()

import httpx
from app.core.database import SessionLocal
from app.core.crypto import decrypt_optional
from app.models import OAuthIdentity

META_APP_SECRET = os.environ["META_APP_SECRET"]
META_APP_ID     = os.environ["META_APP_ID"]
META_API_VERSION = os.environ.get("META_API_VERSION", "v26.0")
BASE = f"https://graph.facebook.com/{META_API_VERSION}"

def appsecret_proof(token: str) -> str:
    return hmac.new(META_APP_SECRET.encode(), token.encode(), hashlib.sha256).hexdigest()

def auth(token: str) -> dict:
    return {"access_token": token, "appsecret_proof": appsecret_proof(token)}

def pp(label: str, data) -> None:
    print(f"\n{'='*60}")
    print(f"  {label}")
    print('='*60)
    print(json.dumps(data, indent=2, default=str))


async def get_db_token(identity_id: int) -> str:
    async with SessionLocal() as session:
        identity = await session.get(OAuthIdentity, identity_id)
        return decrypt_optional(identity.access_token_encrypted) or ""


async def main() -> None:
    # Identity 3 = Karthik K C — the one used by the meta_ads connection
    token = await get_db_token(3)
    print(f"\nUsing DB token for identity 3 (Karthik K C): {token[:40]}...")

    async with httpx.AsyncClient(timeout=30) as client:
        # Platform connection uses act_1226814221486092 (NOT act_1219296233241927 from .env)
        act = "act_1226814221486092"

        # 1. Token debug
        r = await client.get(f"{BASE}/debug_token", params={
            "input_token": token,
            "access_token": f"{META_APP_ID}|{META_APP_SECRET}",
        })
        debug = r.json()
        data = debug.get("data", {})
        print(f"\n>>> Token valid?     {data.get('is_valid')}")
        print(f">>> Expires at:      {data.get('expires_at')}")
        print(f">>> Scopes:          {data.get('scopes', [])}")

        # 2. Ad account info
        r = await client.get(f"{BASE}/{act}", params={
            "fields": "id,name,currency,account_status,disable_reason,timezone_name",
            **auth(token),
        })
        acc = r.json()
        pp(f"Ad account {act} (the one platform is syncing)", acc)
        print(f"\n>>> account_status: {acc.get('account_status')} (1=ACTIVE, need 1)")

        if "error" in acc:
            print(f"\n!!! ERROR accessing {act}: {acc['error']}")
            print(">>> This is likely the root cause — token cannot access this ad account")
            return

        # 3. Campaigns
        r = await client.get(f"{BASE}/{act}/campaigns", params={
            "fields": "id,name,status,objective",
            "limit": 10,
            **auth(token),
        })
        camp_resp = r.json()
        pp(f"{act}/campaigns", camp_resp)
        print(f"\n>>> Campaigns found: {len(camp_resp.get('data', []))}")

        # 4. Insights (the backfill start is 2026-08-08, today is 2026-09-07)
        r = await client.get(f"{BASE}/{act}/insights", params={
            "level": "campaign",
            "fields": "campaign_id,campaign_name,impressions,clicks,spend",
            "time_increment": 1,
            "time_range": json.dumps({"since": "2026-08-08", "until": "2026-09-07"}),
            "limit": 5,
            **auth(token),
        })
        ins_resp = r.json()
        pp(f"{act}/insights (backfill window 2026-08-08 → 2026-09-07)", ins_resp)
        print(f"\n>>> Insight rows: {len(ins_resp.get('data', []))}")
        if "error" in ins_resp:
            print(f"!!! INSIGHTS ERROR: {ins_resp['error']}")

        # 5. Compare the two ad accounts
        print(f"\n{'='*60}")
        print(f"  COMPARISON")
        print('='*60)
        print(f"  .env  META_AD_ACCOUNT_ID:    act_1219296233241927  (has data - verified)")
        print(f"  Platform connection uses:    {act}  (above is what the platform queries)")
        if acc.get("account_status") != 1:
            print(f"\n  ROOT CAUSE: {act} is not ACTIVE (status={acc.get('account_status')})")
        elif len(camp_resp.get("data", [])) == 0 and "error" not in camp_resp:
            print(f"\n  ROOT CAUSE: {act} has NO campaigns")
        elif len(ins_resp.get("data", [])) == 0:
            print(f"\n  ROOT CAUSE: No insights data for {act} in the backfill window")
        else:
            print(f"\n  Data exists! Check if insights show up in DB report_rows")


if __name__ == "__main__":
    asyncio.run(main())
