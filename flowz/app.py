import itertools
import sys

import six
from tornado import gen
from tornado import ioloop

from . import channels

class Flo(object):
    """
    Class for managing data processing workflows.

    A `Flo` is given some number of target
    :class:`flowz.channels.Channel` objects.

    The `Flo` can then be `run()`, and it will run a `tornado.ioloop.IOLoop`
    until all target channels have been consumed, at which point `run()`
    will return control to the caller.

    Unlike typical tornado applications, unhandled exceptions don't get
    caught by the `IOLoop`; rather, unhandled exceptions that make it all
    the way past a given channel will cause the `IOLoop` to stop and the
    exception to propagate to the caller.
    """

    exc_info = None

    def __init__(self, targets, loop=None):
        """
        Initialize a `Flo` with the given `targets` channels.

        If `loop` is not given, a new loop will be created on the current
        thread, via `get_default_ioloop`.
        """
        self.count = itertools.count()
        self.targets = {}
        if loop is None:
            loop = self.get_default_ioloop()
        self.loop = loop
        self.add_targets(targets)


    @classmethod
    def get_default_ioloop(cls):
        loop = ioloop.IOLoop()
        loop.make_current()
        return loop


    def add_targets(self, targets):
        c = self.count
        self.targets.update((next(c), t) for t in targets)


    @gen.coroutine
    def wrap_target(self, target):
        """
        Forces app to stop on unhandled exceptions.
        """
        result = None
        try:
            if hasattr(target, 'future'):
                result = yield target.future()
            else:
                # Assume it's a channel.
                try:
                    while True:
                        yield target.next()
                except channels.ChannelDone:
                    pass
        except Exception as e:
            self.exc_info = sys.exc_info()
            self.loop.stop()
        raise gen.Return(result)


    def run(self):
        """
        Runs the workflow, blocking until completion.

        Completion is achieved when either:
        - an unhandled exception propagates past a channel.
        - all channels have been consumed.

        Once complete, the underlying `ioloop` has been stopped.

        No return value.
        """
        self.loop.spawn_callback(self.main)
        self.loop.start()
        if self.exc_info:
            six.reraise(*self.exc_info)


    @gen.coroutine
    def main(self):
        wrap = self.wrap_target
        while self.targets and getattr(self, 'exc_info') is None:
            yield gen.moment
            targets = self.targets
            self.targets = {}
            yield dict((k, wrap(target))
                for k, target in targets.items())
        self.loop.stop()


