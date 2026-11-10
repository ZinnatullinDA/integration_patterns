import os

from aggregator.state import Aggregator
from common.rabbitmq import consume, invalid, publish


def main():
    state = Aggregator(os.getenv('STATE_PATH', '/data/aggregator.sqlite3'))
    timeout = max(1, int(os.getenv('AGGREGATION_TIMEOUT_SEC', '30')))
    def flush(channel):
        state.diagnostics(timeout)
        state.flush(lambda queue, payload: publish(channel, queue, payload),
                    lambda payload, reason: invalid(channel, 'aggregator', payload, reason))
    def handle(channel, payload):
        state.accept(payload)
        flush(channel)
    try:
        consume('repair.results', 'aggregator', handle, flush)
    finally:
        state.close()


if __name__ == '__main__':
    main()
