from types import SimpleNamespace

import pika
import pytest

from common import rabbitmq


class FakeConnection:
    is_open = True

    def close(self):
        self.is_open = False


class FakeChannel:
    def __init__(self, body, fail_publish=False):
        self.body = body
        self.fail_publish = fail_publish
        self.events = []

    def consume(self, *args, **kwargs):
        yield SimpleNamespace(delivery_tag=123), SimpleNamespace(), self.body
        raise KeyboardInterrupt

    def basic_publish(self, **kwargs):
        self.events.append(('publish', kwargs))
        if self.fail_publish:
            raise pika.exceptions.AMQPConnectionError('disconnected')

    def basic_ack(self, tag):
        self.events.append(('ack', tag))


@pytest.mark.parametrize('body', [b'{broken', b'[]', b'\xff'])
def test_invalid_message_ack_after_confirm(monkeypatch, body):
    channel = FakeChannel(body)
    connection = FakeConnection()
    monkeypatch.setattr(rabbitmq, 'connect', lambda: (connection, channel))
    with pytest.raises(KeyboardInterrupt):
        rabbitmq.consume('repair.requests', 'processor', lambda *args: pytest.fail('invalid JSON reached handler'))
    assert [event[0] for event in channel.events] == ['publish', 'ack']
    properties = channel.events[0][1]['properties']
    assert properties.delivery_mode == 2
    assert properties.content_type == 'application/json'
    assert channel.events[0][1]['mandatory'] is True
    assert channel.events[0][1]['routing_key'] == 'repair.invalid'
    assert not connection.is_open


def test_publication_failure_does_not_ack(monkeypatch):
    channel = FakeChannel(b'{broken', fail_publish=True)
    connection = FakeConnection()
    monkeypatch.setattr(rabbitmq, 'connect', lambda: (connection, channel))
    def stop_retry(*args):
        raise KeyboardInterrupt
    monkeypatch.setattr(rabbitmq.time, 'sleep', stop_retry)
    with pytest.raises(KeyboardInterrupt):
        rabbitmq.consume('repair.requests', 'processor', lambda *args: None)
    assert [event[0] for event in channel.events] == ['publish']
    assert not connection.is_open


def test_success_ack_after_handler(monkeypatch):
    channel = FakeChannel(b'{}')
    monkeypatch.setattr(rabbitmq, 'connect', lambda: (FakeConnection(), channel))
    def handler(*args):
        channel.events.append(('handled', None))
    with pytest.raises(KeyboardInterrupt):
        rabbitmq.consume('repair.requests', 'processor', handler)
    assert [event[0] for event in channel.events] == ['handled', 'ack']
