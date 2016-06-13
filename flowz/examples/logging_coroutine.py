import logging

from tornado import gen
from tornado import ioloop


def wrapper(callable):
    @gen.coroutine
    def call_func():
        try:
            val = yield callable()
        except Exception as e:
            logging.exception(e)
            #ioloop.IOLoop.current().stop()
            raise
        raise gen.Return(True)
    return call_func


@gen.coroutine
def exc_raiser():
    yield gen.moment
    val = 3 / 0
    raise gen.Return(val)



def main():
    loop = ioloop.IOLoop()
    loop.make_current()
    loop.spawn_callback(wrapper(exc_raiser))
    loop.start()
    loop.stop()


if __name__ == '__main__':
    main()
