import json

from client.logging import received
from client.state import open_client_db, request_number
from client.startup import send_startup_requests
from common.rabbitmq import consume
from common.schemas import base, integer, require


def main():
    db = open_client_db()
    db.execute('CREATE TABLE IF NOT EXISTS completed (request_id TEXT PRIMARY KEY, payload TEXT NOT NULL)')
    def handle(channel, payload):
        base(payload)
        require(payload.get('status') == 'completed', 'ожидается готовая смета')
        integer(payload, 'total_cost_rub')
        integer(payload, 'total_duration_days')
        rid = payload['request_id']
        with db:
            inserted = db.execute('INSERT OR IGNORE INTO completed VALUES (?,?)',
                                  (rid, json.dumps(payload, ensure_ascii=False))).rowcount
        received(payload, request_number(db, rid), duplicate=not inserted)
    try:
        send_startup_requests(db)
        consume('repair.completed', 'client', handle)
    finally:
        db.close()


if __name__ == '__main__':
    main()
