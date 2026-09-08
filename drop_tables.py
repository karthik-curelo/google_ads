import asyncio
from app.core.database import engine
from sqlalchemy import text

async def main():
    async with engine.begin() as conn:
        await conn.execute(text("DROP TABLE IF EXISTS google_ads_performance CASCADE"))
        await conn.execute(text("DROP TABLE IF EXISTS meta_ads_performance CASCADE"))
        await conn.execute(text("DROP TABLE IF EXISTS google_analytics_performance CASCADE"))
        await conn.execute(text("DROP TABLE IF EXISTS google_search_console_performance CASCADE"))

asyncio.run(main())
