import json
import logging
import os
import time
from datetime import datetime, timezone
from uuid import uuid4

import pika

from common.logging_config import log
from common.schemas import InvalidMessage, require

QUEUES = ('repair.requests', 'drilling.requests', 'electrical.requests',
          'logistics.requests', 'repair.results', 'repair.completed', 'repair.invalid')


def connect():
    parameters = pika.ConnectionParameters(
        host=os.getenv('RABBITMQ_HOST', 'rabbitmq'),
        credentials=pika.PlainCredentials(os.getenv('RABBITMQ_USER', 'guest'),
                                          os.getenv('RABBITMQ_PASSWORD', 'guest')),
        heartbeat=60, blocked_connection_timeout=30, socket_timeout=10)
    connection = pika.BlockingConnection(parameters)
    channel = connection.channel()
    for queue in QUEUES:
        channel.queue_declare(queue=queue, durable=True)
    channel.basic_qos(prefetch_count=1)
    channel.confirm_delivery()
    log('rabbitmq', None, 'соединение установлено / восстановлено')
    return connection, channel


def publish(channel, queue, payload):
    require(queue in QUEUES, 'неразрешённая очередь')
    try:
        channel.basic_publish(
            exchange='', routing_key=queue, mandatory=True,
            body=json.dumps(payload, ensure_ascii=False).encode('utf-8'),
            properties=pika.BasicProperties(
                delivery_mode=2, content_type='application/json', content_encoding='utf-8',
                message_id=payload.get('message_id', str(uuid4())),
                correlation_id=payload.get('correlation_id'), reply_to=payload.get('reply_to')))
    except pika.exceptions.AMQPError:
        log('rabbitmq', payload.get('request_id'), f'ошибка публикации -> {queue}', logging.ERROR)
        raise


def metadata(payload, properties):
    for field in ('message_id', 'correlation_id', 'reply_to'):
        if field in payload:
            require(getattr(properties, field, None) == payload[field], f'AMQP {field} не совпадает с JSON')


def invalid(channel, stage, payload, reason):
    rid = payload.get('request_id') if isinstance(payload, dict) else None
    publish(channel, 'repair.invalid', dict(stage=stage, reason=str(reason), request_id=rid,
            payload=payload, timestamp=datetime.now(timezone.utc).isoformat()))
    log('validation', rid, f'стадия={stage} | {reason} | -> repair.invalid', logging.ERROR)


def consume(queue, stage, handler, tick=None):
    while True:
        connection = None
        try:
            connection, channel = connect()
            for method, properties, body in channel.consume(queue, inactivity_timeout=1, auto_ack=False):
                if method is not None:
                    payload = body.decode('utf-8', errors='replace')
                    try:
                        payload = json.loads(body)
                        require(isinstance(payload, dict), 'требуется JSON-объект')
                        metadata(payload, properties)
                        handler(channel, payload)
                    except (InvalidMessage, UnicodeDecodeError, json.JSONDecodeError) as exc:
                        invalid(channel, stage, payload, exc)
                    channel.basic_ack(method.delivery_tag)
                if tick:
                    tick(channel)
        except (pika.exceptions.AMQPError, OSError) as exc:
            log('rabbitmq', None, f'соединение потеряно ({type(exc).__name__}), переподключение', logging.ERROR)
            time.sleep(2)
        finally:
            if connection and connection.is_open:
                connection.close()


def send(payload):
    while True:
        connection = None
        try:
            connection, channel = connect()
            publish(channel, 'repair.requests', payload)
            return
        except (pika.exceptions.AMQPError, OSError):
            log('rabbitmq', payload.get('request_id'), 'отправка не подтверждена, повтор через 2 с', logging.ERROR)
            time.sleep(2)
        finally:
            if connection and connection.is_open:
                connection.close()
