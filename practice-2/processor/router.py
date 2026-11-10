from common.schemas import require

ROUTES = {'drilling': 'drilling.requests', 'electrical': 'electrical.requests',
          'logistics': 'logistics.requests'}


def route(work_type):
    require(isinstance(work_type, str) and work_type in ROUTES, 'неизвестный work_type для Router')
    return ROUTES[work_type]
