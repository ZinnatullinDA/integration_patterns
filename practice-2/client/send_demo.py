import argparse
import json
from pathlib import Path

from client.logging import sent
from client.state import open_client_db, request_number
from common.rabbitmq import send


def demo(request_id='REQ-001', well_id='WELL-101'):
    return dict(request_id=request_id, well_id=well_id, reply_to='repair.completed', works=[
        dict(work_type='drilling', quantity=100, unit='meter'),
        dict(work_type='electrical', quantity=3, unit='unit'),
        dict(work_type='logistics', quantity=2, unit='trip')])


def main():
    parser = argparse.ArgumentParser(description='Отправка заявок на ремонт скважины')
    parser.add_argument('--scenario', choices=['happy', 'parallel', 'subset', 'invalid'], default='happy')
    parser.add_argument('--request-id', help='Уникальный ID для нового запуска сценария')
    parser.add_argument('--file', help='JSON-файл произвольной заявки, в том числе невалидной')
    args = parser.parse_args()
    if args.file:
        payloads = [json.loads(Path(args.file).read_text(encoding='utf-8'))]
    elif args.scenario == 'parallel':
        payloads = [demo(args.request_id + '-1' if args.request_id else 'REQ-101', 'WELL-201'),
                    demo(args.request_id + '-2' if args.request_id else 'REQ-102', 'WELL-202')]
        payloads[1]['works'][0]['quantity'] = 20
    else:
        defaults = {'happy': 'REQ-001', 'subset': 'REQ-SUBSET', 'invalid': 'REQ-INVALID'}
        payloads = [demo(args.request_id or defaults[args.scenario])]
        if args.scenario == 'subset':
            payloads[0]['works'].pop()
        if args.scenario == 'invalid':
            del payloads[0]['well_id']
    db = open_client_db()
    try:
        for payload in payloads:
            number = request_number(db, payload.get('request_id'))
            send(payload)
            sent(payload, number)
    finally:
        db.close()


if __name__ == '__main__':
    main()
