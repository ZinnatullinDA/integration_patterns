from common.worker import run


def calculate(quantity):
    return quantity * 40000, quantity


if __name__ == '__main__':
    run('logistics', calculate)
