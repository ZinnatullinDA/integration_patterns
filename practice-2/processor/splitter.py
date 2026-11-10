import json
from uuid import NAMESPACE_URL, uuid5

from common.logging_config import log
from common.schemas import request


def split(payload, emit_logs=True):
    request(payload)
    identity = json.dumps({key: payload[key] for key in ('request_id', 'well_id', 'reply_to', 'works')},
                          sort_keys=True, ensure_ascii=False)
    parts = [dict(work, request_id=payload['request_id'], well_id=payload['well_id'],
                  message_id=str(uuid5(NAMESPACE_URL, identity + ':' + work['work_type'])),
                  reply_to='repair.results') for work in payload['works']]
    if emit_logs:
        log('splitter', payload['request_id'], f'заявка разделена на {len(parts)} части')
        for item in parts:
            log('splitter', payload['request_id'], f"work_type={item['work_type']} | message_id={item['message_id']}")
    return parts
