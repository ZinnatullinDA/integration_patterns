from common.worker import run


def calculate(quantity):
    return quantity * 5000, (quantity + 9) // 10


if __name__ == '__main__':
    run('drilling', calculate)
