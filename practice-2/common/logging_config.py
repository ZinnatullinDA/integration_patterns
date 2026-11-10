import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s | %(levelname)-7s | %(message)s')
logging.getLogger('pika').setLevel(logging.WARNING)


def log(component, request_id, message, level=logging.INFO):
    logging.log(level, '[%s] | request_id=%s | %s', component.upper(), request_id or '-', message)
