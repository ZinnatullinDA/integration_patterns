"""Real RabbitMQ/Compose acceptance test. RUN_INTEGRATION=1 enables it.

Run on the host with the seven services already started. This test stops/restarts
services in the practice-2 Compose project; do not run against shared environments.
COMPOSE_FILE can select a local verification override.
"""
import json
import os
from pathlib import Path
import subprocess
import time
from uuid import uuid4

import pytest

from client.send_demo import demo
from common.rabbitmq import connect, publish
from common.worker import response
from drilling.main import calculate
from processor.splitter import split

pytestmark = pytest.mark.skipif(os.getenv('RUN_INTEGRATION') != '1', reason='RUN_INTEGRATION=1 requires running Compose')
ROOT = Path(__file__).resolve().parents[1]


def compose(*args):
    result = subprocess.run(['docker', 'compose', *args], cwd=ROOT, capture_output=True,
                            text=True, encoding='utf-8', timeout=120)
    assert result.returncode == 0, result.stderr
    return result.stdout


def query(service, sql, parameters=()):
    path = '/data/client.sqlite3' if service == 'client' else '/data/aggregator.sqlite3'
    code = ('import sqlite3,json; db=sqlite3.connect(' + repr(path) + '); '
            'print(json.dumps(db.execute(' + repr(sql) + ',' + repr(parameters) + ').fetchall()))')
    return json.loads(compose('exec', '-T', service, 'python', '-c', code))


def wait_for(callback, timeout=60):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        value = callback()
        if value:
            return value
        time.sleep(.5)
    raise AssertionError('timeout waiting for acceptance condition')


def test_rabbitmq_acceptance(monkeypatch):
    monkeypatch.setenv('RABBITMQ_HOST', 'localhost')
    connection, channel = connect()
    prefix = 'TEST-' + uuid4().hex[:8]
    def send(payload):
        publish(channel, 'repair.requests', payload)
    def completed(rid):
        rows = query('client', 'SELECT payload FROM completed WHERE request_id=?', (rid,))
        return json.loads(rows[0][0]) if rows else None
    try:
        happy = demo(prefix + '-happy')
        send(happy)
        answer = wait_for(lambda: completed(happy['request_id']))
        assert (answer['total_cost_rub'], answer['total_duration_days']) == (730000, 10)

        first, second = demo(prefix + '-parallel1', 'WELL-A'), demo(prefix + '-parallel2', 'WELL-B')
        second['works'][0]['quantity'] = 20
        send(first)
        send(second)
        a = wait_for(lambda: completed(first['request_id']))
        b = wait_for(lambda: completed(second['request_id']))
        assert (a['well_id'], a['total_cost_rub']) == ('WELL-A', 730000)
        assert (b['well_id'], b['total_cost_rub']) == ('WELL-B', 330000)

        subset = demo(prefix + '-subset')
        subset['works'].pop()
        send(subset)
        assert wait_for(lambda: completed(subset['request_id']))['total_cost_rub'] == 650000

        # Full request redelivery and duplicate worker response must preserve the original estimate.
        send(happy)
        duplicate = response(split(happy)[0], 'drilling', calculate)
        publish(channel, 'repair.results', duplicate)
        time.sleep(6)
        assert completed(happy['request_id']) == answer
        assert query('aggregator', 'SELECT COUNT(*) FROM answers WHERE request_id=?', (happy['request_id'],)) == [[3]]

        for index, change in enumerate(('missing-well', 'unknown-work', 'bad-quantity')):
            bad = demo(prefix + '-invalid-' + str(index))
            if change == 'missing-well':
                del bad['well_id']
            elif change == 'unknown-work':
                bad['works'][0]['work_type'] = 'unknown'
            else:
                bad['works'][0]['quantity'] = True
            send(bad)
        # Inspect invalid channel without discarding messages; nack only after scanning a batch.
        reasons = set()
        def invalid_received():
            tags = []
            try:
                for _ in range(1000):
                    method, _, body = channel.basic_get('repair.invalid', auto_ack=False)
                    if method is None:
                        break
                    tags.append(method.delivery_tag)
                    payload = json.loads(body)
                    if str(payload.get('request_id', '')).startswith(prefix):
                        assert payload['stage'] == 'processor' and payload['reason'] and payload['timestamp']
                        reasons.add(payload['request_id'])
            finally:
                for tag in tags:
                    channel.basic_nack(tag, requeue=True)
            return len(reasons) == 3
        wait_for(invalid_received)
        for index in range(3):
            rid = prefix + '-invalid-' + str(index)
            assert query('aggregator', 'SELECT COUNT(*) FROM requests WHERE id=?', (rid,)) == [[0]]

        compose('stop', 'logistics')
        recovery = demo(prefix + '-recovery')
        send(recovery)
        wait_for(lambda: query('aggregator', 'SELECT COUNT(*) FROM answers WHERE request_id=?', (recovery['request_id'],)) == [[2]])
        assert not completed(recovery['request_id'])
        compose('restart', 'aggregator')
        assert query('aggregator', 'SELECT COUNT(*) FROM answers WHERE request_id=?', (recovery['request_id'],)) == [[2]]
        connection.close()
        compose('restart', 'rabbitmq')
        compose('start', 'logistics')
        answer = wait_for(lambda: completed(recovery['request_id']), timeout=90)
        assert (answer['total_cost_rub'], answer['total_duration_days']) == (730000, 10)
        print('A-F passed: happy, parallel, subset, invalid, duplicates, worker/broker/aggregator recovery')
    finally:
        if connection.is_open:
            connection.close()
        compose('start', 'logistics', 'aggregator')
