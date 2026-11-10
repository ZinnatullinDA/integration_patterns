import json
import logging
import sqlite3
import time
from pathlib import Path
from uuid import NAMESPACE_URL, uuid5

from common.logging_config import log
from common.schemas import InvalidMessage, require, request, result
from processor.splitter import split


def encoded(value):
    return json.dumps(value, sort_keys=True, ensure_ascii=False)


class Aggregator:
    def __init__(self, path):
        Path(path).parent.mkdir(parents=True, exist_ok=True)
        self.db = sqlite3.connect(path)
        self.db.execute('PRAGMA journal_mode=WAL')
        self.db.execute('PRAGMA synchronous=FULL')
        self.db.executescript('''
            CREATE TABLE IF NOT EXISTS requests (
                id TEXT PRIMARY KEY, registration TEXT NOT NULL, created REAL NOT NULL,
                estimate TEXT, sent INTEGER NOT NULL DEFAULT 0);
            CREATE TABLE IF NOT EXISTS answers (
                request_id TEXT, work_type TEXT, payload TEXT NOT NULL,
                PRIMARY KEY(request_id, work_type));
            CREATE TABLE IF NOT EXISTS early (
                id TEXT PRIMARY KEY, request_id TEXT, payload TEXT NOT NULL, created REAL NOT NULL);
            CREATE TABLE IF NOT EXISTS invalids (id INTEGER PRIMARY KEY, payload TEXT, reason TEXT);
        ''')
        self.last_warning = 0

    def close(self):
        self.db.close()

    def accept(self, payload):
        with self.db:
            if payload.get('kind') == 'registration':
                self._register(payload)
            else:
                result(payload)
                if self.db.execute('SELECT 1 FROM requests WHERE id=?', (payload['request_id'],)).fetchone():
                    self._answer(payload)
                else:
                    self.db.execute('INSERT OR IGNORE INTO early VALUES (?,?,?,?)',
                                    (payload['message_id'], payload['request_id'], encoded(payload), time.time()))
                    log('aggregator', payload['request_id'], 'ранний ответ сохранён, ожидается регистрация')

    def _register(self, payload):
        original = {key: payload.get(key) for key in ('request_id', 'well_id', 'reply_to', 'works')}
        request(original)
        parts = split(original, emit_logs=False)
        require(payload.get('parts') == parts, 'регистрация содержит неверные подзаявки')
        registration = encoded(dict(original, parts=parts))
        rid = payload['request_id']
        previous = self.db.execute('SELECT registration FROM requests WHERE id=?', (rid,)).fetchone()
        require(previous is None or previous[0] == registration, 'конфликт повторного request_id')
        self.db.execute('INSERT OR IGNORE INTO requests(id,registration,created) VALUES (?,?,?)',
                        (rid, registration, time.time()))
        log('aggregator', rid, 'зарегистрирована заявка | expected=' + ','.join(p['work_type'] for p in parts))
        for message_id, raw in self.db.execute('SELECT id,payload FROM early WHERE request_id=?', (rid,)).fetchall():
            try:
                self._answer(json.loads(raw))
            except InvalidMessage as exc:
                self.db.execute('INSERT INTO invalids(payload,reason) VALUES (?,?)', (raw, str(exc)))
            self.db.execute('DELETE FROM early WHERE id=?', (message_id,))

    def _answer(self, payload):
        result(payload)
        rid = payload['request_id']
        registration = json.loads(self.db.execute('SELECT registration FROM requests WHERE id=?', (rid,)).fetchone()[0])
        expected = {p['work_type']: p for p in registration['parts']}
        kind = payload['work_type']
        require(payload['well_id'] == registration['well_id'], 'well_id не совпадает с регистрацией')
        require(kind in expected, 'неожидаемый work_type')
        require(payload['correlation_id'] == expected[kind]['message_id'], 'неверный correlation_id')
        old = self.db.execute('SELECT payload FROM answers WHERE request_id=? AND work_type=?', (rid, kind)).fetchone()
        if old:
            previous = json.loads(old[0])
            require(all(previous[key] == payload[key] for key in
                        ('correlation_id', 'cost_rub', 'duration_days', 'status')), 'конфликт повторного ответа')
            log('aggregator', rid, f'work_type={kind} | correlation_id={payload["correlation_id"]} | дублирующий ответ проигнорирован', logging.WARNING)
            return
        self.db.execute('INSERT INTO answers VALUES (?,?,?)', (rid, kind, encoded(payload)))
        rows = self.db.execute('SELECT payload FROM answers WHERE request_id=? ORDER BY work_type', (rid,)).fetchall()
        log('aggregator', rid, f'work_type={kind} | correlation_id={payload["correlation_id"]} | progress={len(rows)}/{len(expected)}')
        if len(rows) == len(expected):
            estimates = [{key: answer[key] for key in ('work_type', 'cost_rub', 'duration_days')}
                         for answer in (json.loads(row[0]) for row in rows)]
            estimate = dict(request_id=rid, well_id=registration['well_id'], status='completed',
                            message_id=str(uuid5(NAMESPACE_URL, 'completed:' + rid)), correlation_id=rid,
                            estimates=estimates, total_cost_rub=sum(e['cost_rub'] for e in estimates),
                            total_duration_days=max(e['duration_days'] for e in estimates), duration_rule='parallel_max')
            self.db.execute('UPDATE requests SET estimate=? WHERE id=?', (encoded(estimate), rid))

    def flush(self, publish, invalid):
        for row_id, raw, reason in self.db.execute('SELECT id,payload,reason FROM invalids').fetchall():
            invalid(json.loads(raw), reason)
            with self.db:
                self.db.execute('DELETE FROM invalids WHERE id=?', (row_id,))
        for rid, registration, raw in self.db.execute(
                'SELECT id,registration,estimate FROM requests WHERE estimate IS NOT NULL AND sent=0').fetchall():
            estimate = json.loads(raw)
            publish(json.loads(registration)['reply_to'], estimate)
            with self.db:
                self.db.execute('UPDATE requests SET sent=1 WHERE id=?', (rid,))
            log('aggregator', rid, f"итоговая смета -> repair.completed | total={estimate['total_cost_rub']} RUB | duration={estimate['total_duration_days']} days")

    def diagnostics(self, timeout):
        now = time.time()
        if now - self.last_warning < timeout:
            return
        self.last_warning = now
        for rid, raw in self.db.execute('SELECT id,registration FROM requests WHERE estimate IS NULL AND created<?', (now-timeout,)):
            expected = {p['work_type'] for p in json.loads(raw)['parts']}
            received = {r[0] for r in self.db.execute('SELECT work_type FROM answers WHERE request_id=?', (rid,))}
            log('aggregator', rid, f'ожидание ответов: {len(received)}/{len(expected)}, отсутствует {sorted(expected-received)}', logging.WARNING)
        with self.db:
            for message_id, raw in self.db.execute('SELECT id,payload FROM early WHERE created<?', (now-timeout,)).fetchall():
                self.db.execute('INSERT INTO invalids(payload,reason) VALUES (?,?)', (raw, 'таймаут ожидания регистрации'))
                self.db.execute('DELETE FROM early WHERE id=?', (message_id,))
