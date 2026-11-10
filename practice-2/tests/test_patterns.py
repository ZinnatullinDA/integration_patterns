from types import SimpleNamespace

import pytest

from aggregator.state import Aggregator
from client.send_demo import demo
from common.rabbitmq import metadata
from common.schemas import InvalidMessage, part, request
from common.worker import response
from drilling.main import calculate as drilling
from electrical.main import calculate as electrical
from logistics.main import calculate as logistics
from processor.router import route
from processor.splitter import split

CALCULATORS = {'drilling': drilling, 'electrical': electrical, 'logistics': logistics}


def registration(payload):
    return dict(payload, kind='registration', parts=split(payload))


def replies(payload):
    return [response(p, p['work_type'], CALCULATORS[p['work_type']]) for p in split(payload)]


def flush(state):
    published, invalid = [], []
    state.flush(lambda queue, payload: published.append((queue, payload)),
                lambda payload, reason: invalid.append((payload, reason)))
    return published, invalid


@pytest.mark.parametrize('count', [1, 2, 3])
def test_splitter(count):
    payload = demo()
    payload['works'] = payload['works'][:count]
    parts = split(payload)
    assert len(parts) == count
    assert len({p['message_id'] for p in parts}) == count
    assert split(payload) == parts
    assert all(part(p) and p['request_id'] == payload['request_id'] for p in parts)


@pytest.mark.parametrize('kind', CALCULATORS)
def test_router(kind):
    assert route(kind) == kind + '.requests'


def test_router_unknown():
    with pytest.raises(InvalidMessage):
        route('unknown')


@pytest.mark.parametrize('calculate,quantity,expected', [
    (drilling, 100, (500000, 10)), (drilling, 11, (55000, 2)),
    (electrical, 3, (150000, 3)), (logistics, 2, (80000, 2)),
    (drilling, 1_000_000, (5_000_000_000, 100000))])
def test_calculators(calculate, quantity, expected):
    assert calculate(quantity) == expected
    assert all(type(v) is int for v in calculate(quantity))


@pytest.mark.parametrize('mutate', [
    lambda p: p.pop('well_id'), lambda p: p.update(well_id=' '),
    lambda p: p.update(works=[]), lambda p: p.update(reply_to='evil'),
    lambda p: p['works'][0].update(work_type='unknown'),
    lambda p: p['works'][0].update(quantity=0),
    lambda p: p['works'][0].update(quantity=-1),
    lambda p: p['works'][0].update(quantity=True),
    lambda p: p['works'][0].update(quantity=1.5),
    lambda p: p['works'][0].update(quantity=1_000_001),
    lambda p: p['works'][0].update(unit='trip'),
    lambda p: p['works'].__setitem__(1, p['works'][0].copy()),
    lambda p: p.update(works=[None]), lambda p: p['works'][0].update(work_type=[]),
])
def test_invalid_requests(mutate):
    payload = demo()
    mutate(payload)
    with pytest.raises(InvalidMessage):
        request(payload)


def test_correlations_and_metadata():
    part_message = split(demo())[0]
    answer = response(part_message, 'drilling', drilling)
    assert answer['correlation_id'] == part_message['message_id']
    metadata(answer, SimpleNamespace(message_id=answer['message_id'], correlation_id=answer['correlation_id']))
    with pytest.raises(InvalidMessage):
        metadata(answer, SimpleNamespace(message_id='wrong'))
    with pytest.raises(InvalidMessage):
        response(part_message, 'logistics', logistics)


@pytest.mark.parametrize('count', [1, 2, 3])
def test_completion_and_duplicates(tmp_path, count):
    state = Aggregator(tmp_path / 'state.db')
    payload = demo()
    payload['works'] = payload['works'][:count]
    state.accept(registration(payload))
    answers = replies(payload)
    for answer in answers[:-1]:
        state.accept(answer)
        state.accept(answer)
        assert flush(state)[0] == []
    state.accept(answers[-1])
    published, invalid = flush(state)
    assert not invalid and len(published) == 1
    queue, estimate = published[0]
    assert queue == 'repair.completed'
    assert estimate['total_cost_rub'] == sum(a['cost_rub'] for a in answers)
    assert estimate['total_duration_days'] == max(a['duration_days'] for a in answers)
    if count == 3:
        assert (estimate['total_cost_rub'], estimate['total_duration_days']) == (730000, 10)
    for answer in answers:
        state.accept(answer)
    state.accept(registration(payload))
    assert flush(state)[0] == []
    state.close()


def test_parallel_early_and_restart(tmp_path):
    path = tmp_path / 'state.db'
    state = Aggregator(path)
    first, second = demo('A', 'WELL-A'), demo('B', 'WELL-B')
    second['works'][0]['quantity'] = 20
    a, b = replies(first), replies(second)
    state.accept(a[0])
    state.close()
    state = Aggregator(path)
    state.accept(registration(first))
    state.accept(registration(second))
    state.accept(b[2])
    state.accept(a[1])
    assert not flush(state)[0]
    state.close()
    state = Aggregator(path)
    state.accept(b[0])
    state.accept(a[2])
    state.accept(b[1])
    published, _ = flush(state)
    estimates = {p['request_id']: p for _, p in published}
    assert estimates['A']['well_id'] == 'WELL-A'
    assert estimates['A']['total_cost_rub'] == 730000
    assert estimates['B']['well_id'] == 'WELL-B'
    assert estimates['B']['total_cost_rub'] == 330000
    state.close()


@pytest.mark.parametrize('changes', [dict(correlation_id='fake'), dict(well_id='other'),
                                     dict(cost_rub=1.5), dict(status='failed')])
def test_reject_forged_response(tmp_path, changes):
    state = Aggregator(tmp_path / 'state.db')
    state.accept(registration(demo()))
    answer = replies(demo())[0]
    answer.update(changes)
    with pytest.raises(InvalidMessage):
        state.accept(answer)
    assert not flush(state)[0]
    state.close()


def test_conflicting_request_and_reply(tmp_path):
    state = Aggregator(tmp_path / 'state.db')
    state.accept(registration(demo()))
    changed = demo()
    changed['well_id'] = 'OTHER'
    with pytest.raises(InvalidMessage):
        state.accept(registration(changed))
    answer = replies(demo())[0]
    state.accept(answer)
    with pytest.raises(InvalidMessage):
        state.accept(dict(answer, cost_rub=1))
    state.close()


def test_publish_failure_keeps_pending_estimate(tmp_path):
    state = Aggregator(tmp_path / 'state.db')
    state.accept(registration(demo()))
    for answer in replies(demo()):
        state.accept(answer)
    def fail(*args):
        raise OSError('broker unavailable')
    with pytest.raises(OSError):
        state.flush(fail, fail)
    state.close()
    state = Aggregator(tmp_path / 'state.db')
    assert len(flush(state)[0]) == 1
    assert not flush(state)[0]
    state.close()


def test_invalid_early_reply_and_orphan_timeout(tmp_path):
    state = Aggregator(tmp_path / 'state.db')
    forged = dict(replies(demo())[0], correlation_id='fake')
    state.accept(forged)
    state.accept(registration(demo()))
    assert len(flush(state)[1]) == 1
    state.accept(replies(demo('orphan'))[0])
    state.db.execute('UPDATE early SET created=0')
    state.db.commit()
    state.diagnostics(1)
    assert len(flush(state)[1]) == 1
    assert not flush(state)[0]
    state.close()
