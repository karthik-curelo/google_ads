import asyncio
from httpx import AsyncClient, ASGITransport
from app.main import app
import json

async def run_tests():
    print('Testing GET /api/v1/connections')
    headers = {'Authorization': 'Bearer NB4pct2VV8j24Us4AAKGrZrM78xna8Zn5VF9nEgXNLM'}
    
    async with AsyncClient(transport=ASGITransport(app=app), base_url='http://test') as client:
        response = await client.get('/api/v1/connections', headers=headers)
        print('Status:', response.status_code)
        if response.status_code == 200:
            data = response.json()
            conns = data.get('connections', [])
            print(f'Found {len(conns)} connections.')
            for c in conns:
                cid = c['id']
                name = c['name']
                connector = c['connector_id']
                print(f'\nTesting connection {name} ({cid}) - {connector}')
                r = await client.get(f'/api/v1/connections/{cid}/data?limit=2', headers=headers)
                if r.status_code == 200:
                    d = r.json()
                    print(f'  Success! Table: {d.get("table_name")}')
                    print(f'  Columns: {len(d.get("columns", []))} - {[col.get("name") for col in d.get("columns", [])[:3]]}...')
                    print(f'  Rows: {len(d.get("rows", []))}')
                    if len(d.get("rows", [])) > 0:
                        print(f'  Sample row: {json.dumps(d.get("rows")[0])}')
                    else:
                        print('  No rows returned')
                else:
                    print(f'  Error: {r.status_code} - {r.text}')
        else:
            print('Failed to get connections:', response.text)

if __name__ == '__main__':
    asyncio.run(run_tests())
