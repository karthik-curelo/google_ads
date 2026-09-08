"""
Check the database OAuth identities and connections to see what token
the platform is actually using when it syncs Meta Ads.
"""
import asyncio
import os
import sys

# Add project to path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from dotenv import load_dotenv
load_dotenv()

from sqlalchemy import select, text
from app.core.database import SessionLocal
from app.core.crypto import decrypt_optional
from app.models import OAuthIdentity, Connection


async def main():
    async with SessionLocal() as session:
        # 1. Show all Meta OAuth identities
        result = await session.execute(
            select(OAuthIdentity).where(OAuthIdentity.provider == "meta")
        )
        identities = result.scalars().all()
        print(f"\n{'='*60}")
        print(f"  META OAuth Identities in DB: {len(identities)}")
        print('='*60)
        for identity in identities:
            token = decrypt_optional(identity.access_token_encrypted)
            token_preview = (token[:30] + "...") if token else "NONE"
            expires = identity.access_token_expires_at
            scopes = identity.scopes or []
            print(f"\n  ID:           {identity.id}")
            print(f"  Email:        {identity.email}")
            print(f"  Display Name: {identity.display_name}")
            print(f"  Status:       {identity.status}")
            print(f"  Scopes:       {scopes}")
            print(f"  Token:        {token_preview}")
            print(f"  Token expiry: {expires}")
            print(f"  Status detail:{identity.status_detail}")

        # 2. Show all Meta Ads connections
        result = await session.execute(
            select(Connection).where(Connection.connector_id == "meta_ads")
        )
        connections = result.scalars().all()
        print(f"\n{'='*60}")
        print(f"  META Ads Connections in DB: {len(connections)}")
        print('='*60)
        for conn in connections:
            print(f"\n  Connection ID:    {conn.id}")
            print(f"  Resource ID:      {conn.resource_id}")
            print(f"  Status:           {conn.status}")
            print(f"  Streams:          {conn.streams}")
            print(f"  Backfill start:   {conn.backfill_start_date}")
            print(f"  Lookback days:    {conn.lookback_days}")
            print(f"  Identity ID:      {conn.oauth_identity_id}")
            print(f"  Last run at:      {conn.last_run_at}")
            print(f"  Last success at:  {conn.last_success_at}")
            print(f"  Config:           {conn.config}")
            print(f"  Resource metadata:{conn.resource_metadata}")

        # 3. Show sync state for meta connections
        state_rows = await session.execute(text("""
            SELECT ss.connection_id, ss.stream, ss.cursor_value, ss.updated_at
            FROM sync_state ss
            JOIN connections c ON c.id = ss.connection_id
            WHERE c.connector_id = 'meta_ads'
            ORDER BY ss.connection_id, ss.stream
        """))
        states = state_rows.fetchall()
        print(f"\n{'='*60}")
        print(f"  Stream States for meta_ads connections:")
        print('='*60)
        if states:
            for row in states:
                print(f"  conn={row[0]}  stream={row[1]}  cursor={row[2]}  updated={row[3]}")
        else:
            print("  No stream states found (first run will use default backfill window)")

        # 4. Show the .env META_ACCESS_TOKEN prefix vs what's in DB
        env_token = os.environ.get("META_ACCESS_TOKEN", "NOT SET")
        print(f"\n{'='*60}")
        print(f"  TOKEN COMPARISON")
        print('='*60)
        print(f"  .env META_ACCESS_TOKEN[:40]:  {env_token[:40]}...")
        for identity in identities:
            token = decrypt_optional(identity.access_token_encrypted) or ""
            print(f"  DB identity {identity.id} token[:40]:  {token[:40]}...")
            if token[:40] == env_token[:40]:
                print(f"  >>> MATCH! Identity {identity.id} uses the same token as .env")
            else:
                print(f"  >>> MISMATCH — DB uses a different token than .env META_ACCESS_TOKEN")


if __name__ == "__main__":
    asyncio.run(main())
