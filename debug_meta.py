"""
Diagnostic script to debug Meta Ads data issue.
Checks:
  1. Token validity (debug_token endpoint)
  2. Granted scopes
  3. Ad accounts accessible via /me/adaccounts
  4. Campaigns under the configured ad account
  5. Insights for the ad account (last 90 days)
  6. What's stored in the database for OAuth identities and connections
"""
import asyncio
import hashlib
import hmac
import json
import os
import sys

import httpx
from dotenv import load_dotenv

load_dotenv()

META_ACCESS_TOKEN = os.environ["META_ACCESS_TOKEN"]
META_APP_SECRET   = os.environ["META_APP_SECRET"]
META_APP_ID       = os.environ["META_APP_ID"]
META_AD_ACCOUNT_ID = os.environ["META_AD_ACCOUNT_ID"]   # e.g. "1219296233241927"
META_API_VERSION  = os.environ.get("META_API_VERSION", "v26.0")

BASE = f"https://graph.facebook.com/{META_API_VERSION}"

def appsecret_proof(token: str) -> str:
    return hmac.new(META_APP_SECRET.encode(), token.encode(), hashlib.sha256).hexdigest()

def auth(token: str | None = None) -> dict:
    t = token or META_ACCESS_TOKEN
    return {"access_token": t, "appsecret_proof": appsecret_proof(t)}


def pp(label: str, data) -> None:
    print(f"\n{'='*60}")
    print(f"  {label}")
    print('='*60)
    print(json.dumps(data, indent=2, default=str))


async def main() -> None:
    async with httpx.AsyncClient(timeout=30) as client:

        # ── 1. Token debug info (validity, expiry, scopes) ────────────────
        r = await client.get(f"{BASE}/debug_token", params={
            "input_token": META_ACCESS_TOKEN,
            "access_token": f"{META_APP_ID}|{META_APP_SECRET}",
        })
        debug = r.json()
        pp("debug_token response", debug)

        data = debug.get("data", {})
        is_valid = data.get("is_valid", False)
        print(f"\n>>> Token valid?  {is_valid}")
        print(f">>> Expires at:   {data.get('expires_at', 'n/a')}")
        print(f">>> Scopes:       {data.get('scopes', [])}")
        print(f">>> App ID match: {data.get('app_id')} == {META_APP_ID}? {str(data.get('app_id')) == META_APP_ID}")

        if not is_valid:
            print("\n!!! TOKEN IS INVALID — no data will come back. Reconnect via Meta OAuth.")
            return

        # ── 2. /me adaccounts ────────────────────────────────────────────
        r = await client.get(f"{BASE}/me/adaccounts", params={
            "fields": "id,account_id,name,currency,account_status,timezone_name",
            "limit": 50,
            **auth(),
        })
        pp("/me/adaccounts", r.json())

        # ── 3. Specific ad account ────────────────────────────────────────
        act = f"act_{META_AD_ACCOUNT_ID}" if not META_AD_ACCOUNT_ID.startswith("act_") else META_AD_ACCOUNT_ID
        r = await client.get(f"{BASE}/{act}", params={
            "fields": "id,name,currency,account_status,disable_reason,timezone_name",
            **auth(),
        })
        pp(f"Ad account {act}", r.json())
        acc = r.json()
        print(f"\n>>> account_status: {acc.get('account_status')} (1=ACTIVE)")

        # ── 4. Campaigns ──────────────────────────────────────────────────
        r = await client.get(f"{BASE}/{act}/campaigns", params={
            "fields": "id,name,status,objective,effective_status",
            "limit": 25,
            **auth(),
        })
        camp_resp = r.json()
        pp(f"{act}/campaigns", camp_resp)
        camps = camp_resp.get("data", [])
        print(f"\n>>> Total campaigns returned: {len(camps)}")
        if camps:
            statuses = {c.get("status") for c in camps}
            print(f">>> Campaign statuses: {statuses}")

        # ── 5. Adsets ────────────────────────────────────────────────────
        r = await client.get(f"{BASE}/{act}/adsets", params={
            "fields": "id,name,status,campaign_id,effective_status",
            "limit": 25,
            **auth(),
        })
        adset_resp = r.json()
        pp(f"{act}/adsets", adset_resp)
        print(f"\n>>> Total adsets returned: {len(adset_resp.get('data', []))}")

        # ── 6. Ads ───────────────────────────────────────────────────────
        r = await client.get(f"{BASE}/{act}/ads", params={
            "fields": "id,name,status,adset_id,campaign_id,effective_status",
            "limit": 25,
            **auth(),
        })
        ads_resp = r.json()
        pp(f"{act}/ads", ads_resp)
        print(f"\n>>> Total ads returned: {len(ads_resp.get('data', []))}")

        # ── 7. Insights (last 90 days) — campaign level ───────────────────
        r = await client.get(f"{BASE}/{act}/insights", params={
            "level": "campaign",
            "fields": "campaign_id,campaign_name,impressions,clicks,spend,reach",
            "time_increment": 1,
            "time_range": json.dumps({"since": "2026-06-01", "until": "2026-09-07"}),
            "limit": 10,
            **auth(),
        })
        ins_resp = r.json()
        pp(f"{act}/insights (campaign level, last 90d)", ins_resp)
        insights = ins_resp.get("data", [])
        print(f"\n>>> Total insight rows returned: {len(insights)}")
        if "error" in ins_resp:
            print(f"\n!!! INSIGHTS ERROR: {ins_resp['error']}")

        # ── 8. Summary ────────────────────────────────────────────────────
        print("\n" + "="*60)
        print("  SUMMARY")
        print("="*60)
        print(f"  Token valid:          {is_valid}")
        print(f"  Scopes:               {data.get('scopes', [])}")
        print(f"  Account status:       {acc.get('account_status')} (need 1=ACTIVE)")
        print(f"  Campaigns found:      {len(camps)}")
        print(f"  Adsets found:         {len(adset_resp.get('data', []))}")
        print(f"  Ads found:            {len(ads_resp.get('data', []))}")
        print(f"  Insight rows (90d):   {len(insights)}")

        if not is_valid:
            print("\n  ROOT CAUSE: Token is expired/invalid. Reconnect via OAuth.")
        elif acc.get("account_status") != 1:
            print(f"\n  ROOT CAUSE: Ad account is not ACTIVE (status={acc.get('account_status')}).")
        elif len(camps) == 0:
            print("\n  ROOT CAUSE: No campaigns exist in this ad account yet.")
        elif len(insights) == 0:
            print("\n  ROOT CAUSE: Campaigns exist but no insights data found in the requested date range.")
        else:
            print("\n  Data looks healthy from API side. Check platform connector token source.")


if __name__ == "__main__":
    asyncio.run(main())
