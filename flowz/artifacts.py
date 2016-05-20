from __future__ import absolute_import
from __future__ import print_function

import logging

from tornado import concurrent
from tornado import gen

from flowz import channels


class AbstractArtifact(object):
    """
    An object that wraps the details for asynchronous access to an artifact.
    """

    logger = logging.getLogger('Artifact')
    name = None

    __exists__ = False
    __ensure__ = None
    __get__ = None

    def __init__(self, logger=None, name=None):
        if logger:
            self.logger = logger
        if not name:
            self._as_string = type(self).__name__
        else:
            self._as_string = '%s<%s>' % (type(self).__name__, name)
            self.name = name

    def __str__(self):
        return self._as_string

    def exists(self):
        """
        Returns True if the artifact already exists; False otherwise.
        """
        return self.__exists__

    def ensure(self):
        """
        Returns a Future that will have result True when the artifact's existence is assured.
        """
        if self.__ensure__ is None:
            self.logger.info("Artifact %s starting ensure" % str(self))
            self.__ensure__ = self.__start_ensure__()
        return self.__ensure__

    @gen.coroutine
    def __start_ensure__(self):
        """
        Invoked once to start the asynchronous 'ensure' action.
        """
        yield self.get()
        raise gen.Return(True)

    def get(self):
        """
        Returns a Future the result of which will be the artifact itself.
        """
        if self.__get__ is None:
            self.logger.info("Artifact %s starting get" % str(self))
            self.__get__ = self.__start_get__()
        return self.__get__

    @gen.coroutine
    def __start_get__(self):
        self.__exists__ = True
        raise gen.Return(self)

    def as_channel(self):
        """
        Returns a channel with `self` as its sole item.
        """
        return channels.IterChannel(a for a in (self,))

    def value_channel(self):
        """
        Returns a channel with self's artifact when it's ready.
        """
        return self.as_channel().map(lambda a: a.get()).each_ready()

    def ensure_channel(self):
        """
        Returns a channel with self's ensure() when it's ready.
        """
        return self.as_channel().map(lambda a: a.ensure()).each_ready()


class ExtantArtifact(AbstractArtifact):
    """
    An Artifact that is known to exist and asynchronously retrievable.
    """

    __exists__ = True

    def __init__(self, getter, logger=None, name=None):
        super(ExtantArtifact, self).__init__(logger=logger, name=name)
        self.getter = getter

    @gen.coroutine
    def __start_ensure__(self):
        # It is known to exist, so there's nothing to ensure.
        raise gen.Return(True)

    @gen.coroutine
    def __start_get__(self):
        result = yield self.getter()
        self.logger.info("Extant artifact %s retrieved." % str(self))
        raise gen.Return(result)


class TransformArtifact(AbstractArtifact):
    """
    An Artifact that needs to be derived from some sources.
    """

    def __init__(self, transform, *sources, **kw):
        super(TransformArtifact, self).__init__(
                kw.get('logger'), kw.get('name'))
        self.sources = sources
        self.transform = transform

    @gen.coroutine
    def __start_get__(self):
        self.logger.info("Transform %s waiting on sources." % str(self))
        sources = yield [maybe_artifact(source) for source in self.sources]
        self.logger.info("Transform %s applying transformation." % str(self))
        yield gen.moment
        try:
            result = self.transform(*sources)
        except:
            self.logger.exception("Transformation failure.")
            raise
        self.logger.info("Transform %s ready." % str(self))
        self.sources = None
        raise gen.Return(result)


class ThreadedTransformArtifact(AbstractArtifact):
    """
    An Artifact that does its transformation on a thread pool executor
    """

    def __init__(self, executor, transform, *sources, **kw):
        super(ThreadedTransformArtifact, self).__init__(
                kw.get('logger'), kw.get('name'))
        self.sources = sources
        self.transform = transform
        self.executor = executor

    @concurrent.run_on_executor
    def __transform__(self, *sources):
        try:
            return self.transform(*sources)
        except:
            self.logger.exception("Transformation failure.")
            raise

    @gen.coroutine
    def __start_get__(self):
        sources = yield [maybe_artifact(source) for source in self.sources]
        result = yield self.__transform__(*sources)
        self.sources = None
        self.transform = None
        raise gen.Return(result)


class PassthruTransformArtifact(AbstractArtifact):
    def __init__(self, original, transformer, logger=None, name=None):
        super(PassthruTransformArtifact, self).__init__(logger=logger, name=name)
        self.original = original
        self.transformer = transformer

    def exists(self):
        return self.original.exists()

    def __getattr__(self, attr):
        return getattr(self.original, attr)

    def __getitem__(self, item):
        try:
            return self.original[item]
        except KeyError:
            # Want the KeyError to originate here.
            raise KeyError("No such key: %s" % repr(item))

    def ensure(self):
        return self.original.ensure()

    @gen.coroutine
    def __start_get__(self):
        value = yield maybe_artifact(self.original)
        yield gen.moment
        value = self.transformer(value)
        raise gen.Return(value)


def maybe_artifact(value):
    if hasattr(value, 'get'):
        return value.get()
    return gen.maybe_future(value)
