UNITS = {'drilling': 'meter', 'electrical': 'unit', 'logistics': 'trip'}
MAX_QUANTITY = 1_000_000


class InvalidMessage(ValueError):
    pass


def require(condition, reason):
    if not condition:
        raise InvalidMessage(reason)


def identifier(data, key):
    value = data.get(key)
    require(isinstance(value, str) and bool(value.strip()) and len(value) <= 200,
            f'{key}: требуется непустая строка длиной до 200')


def integer(data, key, maximum=10**15):
    require(type(data.get(key)) is int and 0 < data[key] <= maximum,
            f'{key}: требуется положительное целое <= {maximum}')


def base(data):
    require(isinstance(data, dict), 'требуется JSON-объект')
    identifier(data, 'request_id')
    identifier(data, 'well_id')


def work(data):
    require(isinstance(data, dict), 'работа должна быть объектом')
    require(isinstance(data.get('work_type'), str) and data['work_type'] in UNITS,
            'неизвестный work_type')
    integer(data, 'quantity', MAX_QUANTITY)
    require(data.get('unit') == UNITS[data['work_type']], 'неверная единица измерения')


def request(data):
    base(data)
    require(data.get('reply_to') == 'repair.completed', 'недопустимый reply_to заявки')
    works = data.get('works')
    require(isinstance(works, list) and 1 <= len(works) <= 3, 'works: ожидается 1–3 работы')
    for item in works:
        work(item)
    require(len({w['work_type'] for w in works}) == len(works), 'повтор work_type')
    return data


def part(data, expected_type=None):
    base(data)
    work(data)
    identifier(data, 'message_id')
    require(data.get('reply_to') == 'repair.results', 'недопустимый reply_to подзаявки')
    require(expected_type is None or data['work_type'] == expected_type, 'чужая очередь исполнителя')
    return data


def result(data):
    base(data)
    identifier(data, 'message_id')
    identifier(data, 'correlation_id')
    require(isinstance(data.get('work_type'), str) and data['work_type'] in UNITS, 'неизвестный work_type')
    require(data.get('status') == 'success', 'ожидается status=success')
    integer(data, 'cost_rub')
    integer(data, 'duration_days')
    return data
