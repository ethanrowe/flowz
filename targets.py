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



