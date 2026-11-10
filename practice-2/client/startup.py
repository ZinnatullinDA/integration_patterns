import json
from uuid import uuid4

from client.send_demo import demo
from client.logging import sent
from client.state import initialize_numbers, request_number
from common.rabbitmq import send


def send_startup_requests(db, send_request=send):
    db.execute('''CREATE TABLE IF NOT EXISTS startup_requests (
        request_id TEXT PRIMARY KEY, payload TEXT NOT NULL, sent INTEGER NOT NULL DEFAULT 0
    )''')
    with db:
        if not db.execute('SELECT 1 FROM startup_requests LIMIT 1').fetchone():
            batch = uuid4().hex[:12]
            requests = [demo(f'AUTO-{batch}-{index}', f'WELL-{100 + index}')
                        for index in range(1, 4)]
            requests[1]['works'][0]['quantity'] = 20
            requests[2]['works'].pop()
            db.executemany('INSERT INTO startup_requests(request_id,payload) VALUES (?,?)',
                           [(p['request_id'], json.dumps(p, ensure_ascii=False)) for p in requests])

    initialize_numbers(db)
    for rid, raw in db.execute('SELECT request_id,payload FROM startup_requests WHERE sent=0 ORDER BY request_id').fetchall():
        payload = json.loads(raw)
        number = request_number(db, rid)
        send_request(payload)
        with db:
            db.execute('UPDATE startup_requests SET sent=1 WHERE request_id=?', (rid,))
        sent(payload, number)
