import requests
import json
import time

time.sleep(2)
token = 'NB4pct2VV8j24Us4AAKGrZrM78xna8Zn5VF9nEgXNLM'
r = requests.get('http://127.0.0.1:8001/api/v1/connections', headers={'Authorization': f'Bearer {token}'})
print('Connections status:', r.status_code)
conns = r.json().get('connections', [])
for c in conns:
    cid = c['id']
    name = c['name']
    connector_id = c['connector_id']
    res = requests.get(f'http://127.0.0.1:8001/api/v1/connections/{cid}/data?limit=2', headers={'Authorization': f'Bearer {token}'})
    d = res.json()
    print(f"Conn {cid} ({name} - {connector_id}): table='{d.get('table_name')}' rows={len(d.get('rows', []))} cols={len(d.get('columns', []))}")
