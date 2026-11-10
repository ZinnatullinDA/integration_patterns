from common.worker import run


def calculate(quantity):
    return quantity * 50000, quantity


if __name__ == '__main__':
    run('electrical', calculate)
