from __future__ import absolute_import
from __future__ import print_function

from tornado import concurrent as cc
from tornado import gen
from tornado import ioloop as iol
from tornado import locks

class ChannelDone(Exception):
    pass


class Channel(object):
    __started__ = None
    __done__ = False

    def __init__(self, starter):
        # __future__ is the next guy to read.
        self.__future__ = cc.Future()
        # semaphore constraints reading to one at a time.
        self.__read_blocker__ = locks.Semaphore(1)
        # starter is the guy to call to get it going
        self.starter = starter

   
    def done(self):
        return False


    @gen.coroutine
    def start(self):
        if not self.__started__:
            self.__started__ = True
            iol.IOLoop.current().spawn_callback(self.starter, self)
        raise gen.Return(True)


    @gen.coroutine
    def next(self):
        if self.__done__:
            raise ChannelDone("Channel is done")

        # Only one reader may advance the state at a time.
        yield self.__read_blocker__.acquire()

        if not self.__started__:
            yield self.start()

        try:
            next_f, val = yield self.__future__
            self.__future__ = next_f
        except ChannelDone as e:
            self.__done__ = True
            raise e
        finally:
            self.__read_blocker__.release()

        raise gen.Return(val)


class ReadChannel(Channel):
    def __init__(self, channel):
        super(ReadChannel, self).__init__(self.__reader__)
        self.__channel__ = channel

    
    @gen.coroutine
    def __reader__(self, thischan):
        head = self.__future__
        try:
            while not self.__done__:
                value = yield self.__next_item__()
                next_f = cc.Future()
                head.set_result((next_f, value))
                head = next_f
        except ChannelDone:
            head.set_exception(ChannelDone("Channel is done"))


    @gen.coroutine
    def __next_item__(self):
        value = yield self.__channel__.next()
        raise gen.Return(value)


class FunctionChannel(ReadChannel):
    def __init__(self, channel, transform):
        super(FunctionChannel, self).__init__(channel)
        self.__transform__ = transform


    @gen.coroutine
    def __next_item__(self):
        value = yield self.__channel__.next()
        value = self.__transform__(value)
        raise gen.Return(value)


class FutureChannel(ReadChannel):
    @gen.coroutine
    def __next_item__(self):
        value = yield self.__channel__.next()
        value = yield gen.maybe_future(value)
        raise gen.Return(value)


class ReadyFutureChannel(ReadChannel):
    _read_done = False

    def __init__(self, channel):
        super(ReadyFutureChannel, self).__init__(channel)
        self._waiting = set()
        self.__head__ = self.__future__


    @gen.coroutine
    def __reader__(self, thischan):
        try:
            while not self.__done__:
                value = yield self.__next_item__()
                self.__add_future__(value)
        except ChannelDone:
            self._read_done = True


    def __add_future__(self, f):
        self._waiting.add(f)
        iol.IOLoop.current().add_future(f, self.__item_ready__)


    def __item_ready__(self, f):
        self._waiting.remove(f)
        if not self.__done__:
            r = f.exception()
            if r:
                self.__head__.set_exception(r)
            else:
                head_new, head_old = cc.Future(), self.__head__
                self.__head__ = head_new
                head_old.set_result((head_new, f.result()))

                if self._read_done and not self._waiting:
                    self.__head__.set_exception(ChannelDone("Channel is done"))


    @gen.coroutine
    def __next_item__(self):
        value = yield self.__channel__.next()
        # Make sure it's a future to ease downstream use.
        raise gen.Return(gen.maybe_future(value))


class TeeChannel(Channel):
    __started__ = None

    def __init__(self, channel):
        super(TeeChannel, self).__init__(channel.start)
        self.__future__ = channel.__future__


    @gen.coroutine
    def start(self):
        if not self.__started__:
            self.__started__ = True
            yield self.starter()


class ProducerChannel(Channel):
    def __init__(self, starter):
        super(ProducerChannel, self).__init__(starter)
        # head is the next guy to write.
        self.__head__ = self.__future__
        # ready indicates that we're good to go.
        self.__ready__ = cc.Future()


    @gen.coroutine
    def start(self):
        if not self.__started__:
            self.__ready__.set_result(True)
            super(ProducerChannel, self).start()
        raise gen.Return(True)


    @gen.coroutine
    def put(self, item):
        yield self.__ready__
        last_f = self.__head__
        if last_f.done():
            raise ChannelDone()
        next_f = cc.Future()
        self.__head__ = next_f
        last_f.set_result((next_f, item))
        raise gen.Return(True)


    def close(self):
        if not self.__head__.done():
            self.__head__.set_exception(ChannelDone("Channel is done"))


class IterChannel(ProducerChannel):
    def __init__(self, iterable):
        super(IterChannel, self).__init__(self.get_starter(iterable))

    @classmethod
    def get_starter(cls, iterable):
        @gen.coroutine
        def starter(chan):
            yield chan.__ready__
            for value in iterable:
                yield chan.put(value)
            chan.close()
        return starter



if __name__ == '__main__':
    from tornado import ioloop as iol
    loop = iol.IOLoop.current()


    @gen.coroutine
    def starter(chan):
        print("Starting channel", chan)
        for i in range(5):
            print("Producer sleeping.")
            yield gen.sleep(0.1)
            print("Putting value", i)
            yield chan.put(i)
        print("Done writing to channel.")
        chan.close()

    @gen.coroutine
    def reader(chan, name, sleep=0.0):
        print("Reader", name, "sleeping for", sleep)
        yield gen.sleep(sleep)
        print("Reader", name, "looping")
        children = []
        try:
            i = 0
            while i < 10:
                val = yield chan.next()
                print("Reader", name, "received:", val)
                kid = reader(
                        FunctionChannel(TeeChannel(chan), lambda v: v * -1),
                        name + (val,))
                children.append(kid)
                i += 1
        except ChannelDone:
            print("Reader", name, "sees channel is done.")
        print("Reader", name, "waiting on children")
        yield children
        print("Reader", name, "done.")


    @gen.coroutine
    def main():
        def items(n):
            for i in range(n):
                f = cc.Future()
                f.set_result(i)
                yield f

        import random
        @gen.coroutine
        def sleeper(i):
            yield gen.sleep(random.random() * 3)
            raise gen.Return(i)

        channel = ReadyFutureChannel(IterChannel(sleeper(x) for x in range(5)))
        a = reader(ReadChannel(TeeChannel(channel)), ("a",), sleep=0.1)
        b = reader(ReadChannel(TeeChannel(channel)), ("b",), sleep=0.2)
        c = reader(ReadChannel(TeeChannel(channel)), ("c",), sleep=0.3)
        yield [a, b, c]


    print("Starting loop.")
    loop.run_sync(main)
    print("Loop done.")



