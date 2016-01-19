import itertools

from tornado import gen

class Target(object):
    __future__ = None

    @gen.coroutine
    def start(self):
        raise NotImplementedError("You need to implement start yourself.")

    def future(self):
        if self.__future__ is None:
            self.__future__ = self.start()
        return self.__future__


class CallTarget(Target):
    def __init__(self, targetcallable):
        self.target = targetcallable

    @gen.coroutine
    def start(self):
        r = self.target()
        raise gen.Return(r)


class FuncTarget(Target):
    def __init__(self, func, *argets, **kwargets):
        self.func = func
        self.argets = argets
        self.kwargets = kwargets


    @gen.coroutine
    def start(self):
        args, kwargs = yield self.gather_arguments()
        r = self.func(*args, **kwargs)
        raise gen.Return(r)


    @gen.coroutine
    def gather_arguments(self):
        # To impose a particular order on the keywords, we'll use the
        # natural ordering of the keys.
        a = self.argets
        kw = self.kwargets

        inputs = itertools.chain(a, kw.values())
        inputs = yield [t.future() for t in inputs]

        kw = dict(zip(kw.keys(), inputs[len(a):]))
        a = inputs[:len(a)]
        raise gen.Return((a, kw))

