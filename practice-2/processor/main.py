from common.logging_config import log
from common.rabbitmq import consume, publish
from processor.router import route
from processor.splitter import split


def handle(channel, payload):
    log('processor', payload.get('request_id'), 'получена комплексная заявка')
    parts = split(payload)
    publish(channel, 'repair.results', dict(kind='registration', **payload, parts=parts))
    for item in parts:
        queue = route(item['work_type'])
        publish(channel, queue, item)
        log('router', item['request_id'], f"work_type={item['work_type']} | -> {queue}")


if __name__ == '__main__':
    consume('repair.requests', 'processor', handle)
