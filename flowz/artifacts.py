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

    logger = logging.getLogger(__name__)
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
            self.logger.debug("%s starting ensure" % str(self))
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
            self.logger.debug("%s starting get" % str(self))
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
        """
        Create an artifact that is known to exist and asynchronously retrievable.

        @param getter: An asynchronous coroutine to get the value
        """
        super(ExtantArtifact, self).__init__(logger=logger, name=name)
        self.getter = getter

    @gen.coroutine
    def __start_ensure__(self):
        # It is known to exist, so there's nothing to ensure.
        raise gen.Return(True)

    @gen.coroutine
    def __start_get__(self):
        try:
            result = yield self.getter()
        except:
            self.logger.exception("%s getter failure." % str(self))
            raise
        self.logger.debug("%s retrieved." % str(self))
        raise gen.Return(result)


class DerivedArtifact(AbstractArtifact):
    """
    An Artifact that needs to be derived from some sources.
    """

    def __init__(self, deriver, *sources, **kw):
        """
        Create an artifact that needs to be derived from some sources.

        @param deriver: a synchronous function to derive the value
        @param sources: zero or more sources that can be synchronous or asychronous
        """
        super(DerivedArtifact, self).__init__(
                kw.get('logger'), kw.get('name'))
        self.sources = sources
        self.deriver = deriver

    @gen.coroutine
    def __start_get__(self):
        self.logger.debug("%s waiting on sources." % str(self))
        sources = yield [maybe_artifact(source) for source in self.sources]
        self.logger.debug("%s running deriver." % str(self))
        yield gen.moment
        try:
            result = self.deriver(*sources)
        except:
            self.logger.exception("%s deriver failure." % str(self))
            raise
        self.__exists__ = True
        self.logger.debug("%s ready." % str(self))
        self.sources = None
        self.deriver = None
        raise gen.Return(result)


class ThreadedDerivedArtifact(DerivedArtifact):
    """
    A DerivedArtifact that does its derivation on a thread pool executor
    """

    def __init__(self, executor, deriver, *sources, **kw):
        """
        Create an artifact that does its derivation on a thread pool executor
        @param executor: the thread pool executor on which to run
        @param deriver: a synchronous function to derive the value
        @param sources: zero or more sources that can be synchronous or asychronous
        """
        super(ThreadedDerivedArtifact, self).__init__(deriver, *sources, **kw)
        self.logger.debug("%s created (%s)." % (str(self), self.name))
        self.executor = executor

    @concurrent.run_on_executor
    def __derive__(self, *sources):
        self.logger.debug("%s running deriver on executor." % str(self))
        try:
            return self.deriver(*sources)
        except:
            self.logger.exception("%s deriver failure." % str(self))
            raise

    @gen.coroutine
    def __start_get__(self):
        self.logger.debug("%s waiting on sources." % str(self))
        sources = yield [maybe_artifact(source) for source in self.sources]
        result = yield self.__derive__(*sources)
        self.__exists__ = True
        self.logger.debug("%s ready." % str(self))
        self.sources = None
        self.deriver = None
        raise gen.Return(result)


class WrappedArtifact(AbstractArtifact):
    """
    An artifact that wraps another artifact (the "value"), passing through most
    calls to it.

    This class is effectively abstract, since it provides little value when directly used.
    """

    def __init__(self, value, logger=None, name=None):
        """
        Create an artifact that wraps another artifact (the "value"), passing through most
        calls to it.

        @param value: another artifact
        """
        super(WrappedArtifact, self).__init__(logger=logger, name=name)
        self.value = value

    def exists(self):
        return self.value.exists()

    def ensure(self):
        return self.value.ensure()

    def __getattr__(self, attr):
        # NOTE: The text below used to be...
        # if hasattr(self, 'value'):
        # ...but that breaks under Python 3, which seems to immediately call __getattr__ again.
        if 'value' in self.__dict__:
            return getattr(self.value, attr)
        else:
            raise AttributeError("No such attribute: %r; value not yet initialized" % attr)

    def __getitem__(self, item):
        try:
            return self.value[item]
        except KeyError:
            # Want the KeyError to originate here.
            raise KeyError("No such key: %s" % repr(item))

    @gen.coroutine
    def __start_get__(self):
        value = yield maybe_artifact(self.value)
        raise gen.Return(value)


class TransformedArtifact(WrappedArtifact):
    """
    A WrappedArtifact that transforms the value of the wrapped artifact
    """

    def __init__(self, value, transformer=lambda x: x, logger=None, name=None):
        """
        Create an artifact that transforms the value of its wrapped artifact.

        @param value: another artifact
        @param transformer: a synchronous function
        """
        super(TransformedArtifact, self).__init__(value, logger=logger, name=name)
        self.transformer = transformer

    @gen.coroutine
    def __start_get__(self):
        value = yield maybe_artifact(self.value)
        yield gen.moment
        try:
            value = self.transformer(value)
        except:
            self.logger.exception("%s transformer failure." % str(self))
            raise
        raise gen.Return(value)


def maybe_artifact(value):
    # Still duck typing for "artifacts", for now
    if hasattr(value, 'get') and hasattr(value, 'exists') and hasattr(value, 'ensure'):
        return value.get()
    return gen.maybe_future(value)


class KeyedArtifact(WrappedArtifact):
    """
    A WrappedArtifact that knows the key of its original artifact,
    even before the original artifact has been resolved.

    * `key`: the logical key (often a single point from a quantized
        logical or observational time dimension) for identifying this
        item in a sequence, for cogrouping operations, etc.  Its meaning
        and structure is domain-specific.  The key must not change
        during the lifetime of the artifact.

    * `original`: the wrapped artifact.

    All attributes can be accessed both as normal attributes and as
    dictionary keys.  Thus `item.key is item["key"]` and so on.

    Additionally, `item[0]` will give you the key and `item[1]` will give
    the item itself, and `iter(item)` will give you the sequence
    `(item.key, item)`.

    This allows `KeyedArtifact` instances to be used as if they were key,value
    pairs, directly, for things like cogrouping.
    """

    def __init__(self, key, value, logger=None, name=None):
        super(KeyedArtifact, self).__init__(value, logger=logger, name=name)
        self.key = key

    def __getitem__(self, idx):
        if idx == 0:
            return self.key
        elif idx == 1:
            return self
        try:
            return getattr(self, idx)
        except AttributeError:
            raise KeyError("No such key: %s" % repr(idx))

    def __iter__(self):
        return iter((self.key, self))

    def transform(self, func, *params, **kw):
        """
        Create a KeyedArtifact that transforms the value of this artifact, but preserves
        the same key.

        @param func: a synchronous function
        @param params: the initial parameter to func, to which this artifacts value will be appended
        """
        params += (self.value,)
        return KeyedArtifact(self.key, DerivedArtifact(func, *params, **kw))

    def threaded_transform(self, executor, func, *params, **kw):
        """
        Create a KeyedArtifact that transforms the value of this artifact, but preserves
        the same key.  The transformation will run on a separate thread.

        @param executor: the thread pool executor
        @param func: a synchronous function
        @param params: the initial parameter to func, to which this artifacts value will be appended
        """
        params += (self.value,)
        return KeyedArtifact(self.key, ThreadedDerivedArtifact(executor, func, *params, **kw))
