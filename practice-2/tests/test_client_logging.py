from concurrent.futures import ProcessPoolExecutor
import json
import logging
import multiprocessing
import sqlite3

from client import send_demo
from client.logging import received, sent
from client.state import initialize_numbers, open_client_db, request_number
from client.startup import send_startup_requests


def allocate_in_process(arguments):
    path, rid = arguments
    db = sqlite3.connect(path, timeout=30)
    try:
        initialize_numbers(db)
        return rid, request_number(db, rid)
    finally:
        db.close()


def test_parallel_processes_share_unique_numbers(tmp_path):
    path = str(tmp_path / 'client.db')
    ids = [f'REQ-{index}' for index in range(8)] * 3
    with ProcessPoolExecutor(max_workers=4, mp_context=multiprocessing.get_context('spawn')) as pool:
        assigned = list(pool.map(allocate_in_process, [(path, rid) for rid in ids]))
    mapping = dict(assigned)
    assert len(mapping) == 8
    assert set(mapping.values()) == set(range(1, 9))
    assert all(number == mapping[rid] for rid, number in assigned)
    assert allocate_in_process((path, 'REQ-NEXT')) == ('REQ-NEXT', 9)


def test_existing_startup_batch_and_cli_continue_after_restart(tmp_path, monkeypatch):
    path = tmp_path / 'client.db'
    db = sqlite3.connect(path)
    db.execute('CREATE TABLE startup_requests (request_id TEXT PRIMARY KEY, payload TEXT, sent INTEGER)')
    for index in range(1, 4):
        payload = send_demo.demo(f'AUTO-batch-{index}')
        db.execute('INSERT INTO startup_requests VALUES (?,?,1)',
                   (payload['request_id'], json.dumps(payload)))
    db.commit()
    db.close()
    monkeypatch.setenv('CLIENT_STATE_PATH', str(path))
    db = open_client_db()
    assert [request_number(db, f'AUTO-batch-{index}') for index in range(1, 4)] == [1, 2, 3]
    assert request_number(db, 'MANUAL-1') == 4
    assert request_number(db, 'MANUAL-1') == 4
    db.close()
    db = open_client_db()
    assert request_number(db, 'MANUAL-1') == 4
    assert request_number(db, 'MANUAL-2') == 5
    db.close()


def test_startup_cli_and_reply_log_same_number(tmp_path, monkeypatch, caplog):
    monkeypatch.setenv('CLIENT_STATE_PATH', str(tmp_path / 'client.db'))
    db = open_client_db()
    payloads = []
    with caplog.at_level(logging.INFO, logger='client'):
        send_startup_requests(db, payloads.append)
        monkeypatch.setattr(send_demo, 'send', payloads.append)
        monkeypatch.setattr('sys.argv', ['send_demo', '--request-id', 'MANUAL-1'])
        send_demo.main()
        estimate = dict(request_id='MANUAL-1', well_id='WELL-101', total_cost_rub=730000,
                        total_duration_days=10, estimates=[dict(work_type='drilling', cost_rub=500000)])
        received(estimate, request_number(db, 'MANUAL-1'))
    messages = [r.getMessage() for r in caplog.records if r.name == 'client']
    for number in range(1, 5):
        assert any(f'Заявка №{number} отправлена' in message for message in messages)
    assert any('Смета по заявке №4 получена' in message and 'request_id=MANUAL-1' in message for message in messages)
    assert all('request_id=' in message for message in messages)
    assert all('JSON=' not in message and 'quantity' not in message and 'cost_rub' not in message for message in messages)
    assert any('works=drilling,electrical,logistics' in message for message in messages)
    assert payloads[-1] == send_demo.demo('MANUAL-1')  # No number added to the message.
    db.close()


def test_full_json_is_debug_only(caplog):
    payload = send_demo.demo()
    estimate = dict(request_id=payload['request_id'], well_id=payload['well_id'],
                    total_cost_rub=730000, total_duration_days=10,
                    estimates=[dict(work_type='drilling', cost_rub=500000)])
    with caplog.at_level(logging.DEBUG, logger='client'):
        sent(payload, 7)
        received(estimate, 7)
        received(estimate, 7, duplicate=True)
    info = [r.getMessage() for r in caplog.records if r.levelno == logging.INFO]
    debug = [r.getMessage() for r in caplog.records if r.levelno == logging.DEBUG]
    assert len(info) == 3 and len(debug) == 3
    assert all('№7' in message and 'request_id=REQ-001' in message for message in info + debug)
    assert all('JSON=' not in message for message in info)
    assert json.loads(debug[0].split('JSON=', 1)[1]) == payload
    assert json.loads(debug[1].split('JSON=', 1)[1]) == estimate


def test_invalid_request_id_does_not_block_business_validation(tmp_path, monkeypatch):
    path = tmp_path / 'invalid.json'
    payload = send_demo.demo()
    del payload['request_id']
    path.write_text(json.dumps(payload), encoding='utf-8')
    monkeypatch.setenv('CLIENT_STATE_PATH', str(tmp_path / 'client.db'))
    monkeypatch.setattr('sys.argv', ['send_demo', '--file', str(path)])
    published = []
    monkeypatch.setattr(send_demo, 'send', published.append)
    send_demo.main()
    assert published == [payload]
