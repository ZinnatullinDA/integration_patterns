import os
from uuid import uuid4

from common.logging_config import log
from common.rabbitmq import consume, publish
from common.schemas import part


def response(payload, work_type, calculate):
    part(payload, work_type)
    cost, duration = calculate(payload['quantity'])
    return dict(message_id=str(uuid4()), correlation_id=payload['message_id'],
                request_id=payload['request_id'], well_id=payload['well_id'],
                work_type=work_type, cost_rub=cost, duration_days=duration, status='success')


def run(work_type, calculate):
    delay = float(os.getenv(work_type.upper() + '_DELAY_SEC', '0'))
    def handle(channel, payload):
        part(payload, work_type)
        rid = payload['request_id']
        log(work_type, rid, f'work_type={work_type} | расчёт начат')
        channel.connection.sleep(max(0, delay))
        answer = response(payload, work_type, calculate)
        log(work_type, rid, f"work_type={work_type} | расчёт завершён | cost={answer['cost_rub']} RUB | duration={answer['duration_days']} days")
        publish(channel, payload['reply_to'], answer)
        log(work_type, rid, f"work_type={work_type} | -> {payload['reply_to']} | correlation_id={answer['correlation_id']}")
    consume(work_type + '.requests', work_type, handle)
