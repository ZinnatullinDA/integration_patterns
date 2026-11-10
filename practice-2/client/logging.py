import json
import logging
import os

from common.logging_config import log as _configure_logging 

logger = logging.getLogger('client')
logger.setLevel(os.getenv('CLIENT_LOG_LEVEL', 'INFO').upper())


def work_types(items):
    if not isinstance(items, list):
        return '-'
    return ','.join(item['work_type'] for item in items
                    if isinstance(item, dict) and isinstance(item.get('work_type'), str)) or '-'


def sent(payload, number):
    rid = payload.get('request_id')
    logger.info('[CLIENT] | request_id=%s | Заявка №%s отправлена | well_id=%s | works=%s | -> repair.requests',
                rid, number, payload.get('well_id'), work_types(payload.get('works')))
    logger.debug('[CLIENT] | request_id=%s | Заявка №%s | JSON=%s',
                 rid, number, json.dumps(payload, ensure_ascii=False))


def received(payload, number, duplicate=False):
    rid = payload['request_id']
    if duplicate:
        logger.info('[CLIENT] | request_id=%s | Повторная смета по заявке №%s проигнорирована', rid, number)
    else:
        logger.info('[CLIENT] | request_id=%s | Смета по заявке №%s получена | well_id=%s | works=%s | total=%s RUB | duration=%s days',
                    rid, number, payload['well_id'], work_types(payload.get('estimates')),
                    payload['total_cost_rub'], payload['total_duration_days'])
    logger.debug('[CLIENT] | request_id=%s | Смета по заявке №%s | JSON=%s',
                 rid, number, json.dumps(payload, ensure_ascii=False))
