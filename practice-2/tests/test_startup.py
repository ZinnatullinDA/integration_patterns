import sqlite3

import pytest

from client.startup import send_startup_requests
from common.schemas import request


def test_three_requests_once_across_restart(tmp_path):
    path = tmp_path / 'client.db'
    sent = []
    with sqlite3.connect(path) as db:
        send_startup_requests(db, sent.append)
    assert len(sent) == 3
    assert len({p['request_id'] for p in sent}) == 3
    assert [p['well_id'] for p in sent] == ['WELL-101', 'WELL-102', 'WELL-103']
    assert [len(p['works']) for p in sent] == [3, 3, 2]
    for payload in sent:
        request(payload)
    with sqlite3.connect(path) as db:
        send_startup_requests(db, sent.append)
    assert len(sent) == 3


def test_failure_resumes_with_persisted_ids(tmp_path):
    path = tmp_path / 'client.db'
    attempts = []
    def fail_second(payload):
        attempts.append(payload)
        if len(attempts) == 2:
            raise OSError('broker unavailable')
    with sqlite3.connect(path) as db:
        with pytest.raises(OSError):
            send_startup_requests(db, fail_second)
        stored = db.execute('SELECT request_id FROM startup_requests ORDER BY request_id').fetchall()
        assert db.execute('SELECT COUNT(*) FROM startup_requests WHERE sent=1').fetchone()[0] == 1
    resumed = []
    with sqlite3.connect(path) as db:
        send_startup_requests(db, resumed.append)
        assert db.execute('SELECT COUNT(*) FROM startup_requests WHERE sent=1').fetchone()[0] == 3
    assert len(resumed) == 2
    assert resumed[0] == attempts[1]
    assert [p['request_id'] for p in attempts[:1] + resumed] == [row[0] for row in stored]


def test_new_client_volume_gets_new_ids(tmp_path):
    batches = []
    for name in ('first.db', 'second.db'):
        sent = []
        with sqlite3.connect(tmp_path / name) as db:
            send_startup_requests(db, sent.append)
        batches.append({p['request_id'] for p in sent})
    assert batches[0].isdisjoint(batches[1])
