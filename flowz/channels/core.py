from __future__ import absolute_import
from __future__ import print_function

import sys

from tornado import concurrent as cc
from tornado import gen
from tornado import ioloop as iol
from tornado import locks

from flowz import util
from flowz import compat


class ChannelDone(Exception):
    """
    Exception throw when trying to access a completed channel.

    This is akin to StopIteration, but channel-oriented.  We can't
    use StopIteration because `tornado.gen.coroutine` swallows those.
    """
    pass


def set_channel_done_exception(fut, loc):
    """
    HACK This tweaks the internal state of the future in such a way that:
      -- it is still regarded as representing an exception, but
      -- its traceback logger no longer has a formatted_tb value
    The reason for this is to suppress the checks done by tornado
      (in tornado.concurrent._TracebackLogger.__del__)
      that print out "Future exception was never retrieved" messages.
    This is highly reliant on internals of tornado that might change,
      which is why it is wrapped in a broad try-except.

    @param fut: the future on which to set the `ChannelDone` exception
    @param loc: the location in the code (for logging purposes)
    """
    try:
        fut.set_exception(ChannelDone("Channel is done (%s)" % loc))
        fut._tb_logger.formatted_tb = None
    except:
        pass


class Channel(object):
    """
    An asynchronous, dependency-oriented message transport.

    A `Channel` gives a means of asynchronous iteration over messages
    coming from some upstream source.  A consumer of a `Channel` uses its
    `next` method to iteratively receive messages as the channel makes
    them available; when the channel is exhausted, subsequent invocations
    of `next` result in a `ChannelDone` exception.

    A `Channel` has some data source; in the case of this base `Channel` class,
    the `starter` callable passed to `__init__` is expected to kick off an
    asynchronous data producer which will make new values available.  Extensions
    to `Channel` often use another `Channel` as their source.  Whatever the source,
    all follow a basic rule:

    The `Channel`'s producer is not started until something is attempting to
    read from the `Channel` via `next`.

    This is what makes the channels "dependency-oriented"; a `Channel` may be
    created but it won't start doing work until something asks for the results
    of that work.

    A given `Channel` has one current state; it behaves like a traditional queue
    in the case of multiple consumers (each value emitted by the channel is seen
    by one and only one consumer, in non-deterministic fashion).

    See the `TeeChannel` implementation which allows multiple independent consumers
    of a given channel to all see all values; this facilitates dependency-oriented
    programming further by allowing for multiple dependents of the same upstream
    channel.  The data structure representing the message sequence is specifically
    designed to allow for this.

    Most `Channel` implementations preserve the order of their data source; however,
    implementations may vary in this, depending on their purpose.
    """

    __started__ = None
    __done__ = False

    def __init__(self, starter):
        """
        Initializes the `Channel` to call `starter` on activation.

        The `starter` param needs to be callable, and will be invoked with
        the channel itself as the sole parameter.

        Invocation is done on the `tornado.ioloop.IOLoop`, and consequently
        the `starter` needs to be implemented in a manner sensitive to that;
        either spawning other callbacks and exiting quickly, or iteratively
        yielding control back to the `IOLoop` as with `tornado.gen.coroutine`.
        """

        # __future__ is the next guy to read.
        self.__future__ = cc.Future()
        # semaphore constrains reading to one at a time.
        self.__read_blocker__ = locks.Semaphore(1)
        # starter is the guy to call to get it going
        self.starter = starter


    def done(self):
        """
        Returns `True` if all messages have been consumed.
        """
        return self.__done__


    @gen.coroutine
    def start(self):
        """
        Activates the channel so it can start doing work.

        It's critical that `Channel` implementations with a custom `start`
        implementation allow for the possibility that `start` gets invoked
        multiple times, but the actual underlying starter logic must only be
        invoked once.  Custom implementations should use the `self.__start__`
        attribute for this purpose.
        """
        if not self.__started__:
            self.__started__ = True
            iol.IOLoop.current().spawn_callback(self.starter, self)
        raise gen.Return(True)


    @gen.coroutine
    def next(self):
        """
        Asynchronously receives the next message from the channel.

        This returns a `tornado.concurrent.Future` which, when ready, will
        either have the next message as its result, or will have an exception.

        The `ChannelDone` exception indicates that the channel will produce no
        further values.

        Use of `next` causes a Channel to be started if it hasn't already; therefore,
        do not being use of `next` on a channel until you're sure your full dependency
        graph is appropriately in place.

        A typical consumer might look like:

           ```
           @tornado.gen.coroutine
           def reader(channel):
               try:
                   while True:
                       msg = yield channel.next()
                       yield process_message(msg)
               except ChannelDone:
                   pass
           ```

        """
        if self.__done__:
            raise ChannelDone("Channel is done (Channel.next)")

        # Only one reader may advance the state at a time.
        yield self.__read_blocker__.acquire()

        if not self.__started__:
            yield self.start()

        try:
            next_f, read_f, val = yield self.__future__
            # Indicate that this guy has been read.
            if not read_f.done():
                read_f.set_result(True)
            self.__future__ = next_f
        except ChannelDone as e:
            self.__done__ = True
            raise e
        finally:
            self.__read_blocker__.release()

        raise gen.Return(val)


    def tee(self):
        """
        Returns a `TeeChannel` of self, for multiple read paths.

        The resulting channel will emit all the same values as `self` from
        the point of instantiation forward.
        """
        return TeeChannel(self)


    def map(self, mapper):
        """
        Returns a `MapChannel` of self with the given `mapper` function.

        In the resulting channel, the `mapper` will be called per item and
        its result emitted.
        """
        return MapChannel(self, mapper)


    def flat_map(self, mapper):
        """
        Returns a `FlatMapChannel` of self with the given `mapper` function.

        In the resulting channel, the `mapper` will be called per item and is
        expected to produce an iterable; each item in the iterable will be
        emitted (in order) from the channel.
        """
        return FlatMapChannel(self, mapper)


    def filter(self, predicate):
        """
        Returns a `FilterChannel` of self with the given `predicate` function.

        In the resulting channel, the `predicate` will be called per item and
        is expected to produce a truth value; items for which `predicate`
        produces a non-true result will be discarded.
        """
        return FilterChannel(self, predicate)


    def each_ready(self):
        """
        Emit result of each future in order.

        In the resulting `FutureChannel`, each item in `self` is emitted after
        it is ready; if the item is a `tornado.concurrent.Future`, it will be
        waited on and its result emitted.  If the item is not a future, it is
        emitted right away.

        This maintains order of the original channel, so if some futures take
        longer than others, they can act as a bottleneck.
        """
        return FutureChannel(self)


    def as_ready(self):
        """
        Emit channel items as futures become ready.

        In the resulting `ReadyFutureChannel`, the items in `self` are emitted
        as they become ready, independent of their original channel order.

        Items that are `tornado.concurrent.Future` will be waited on; items that
        are not are considered "ready" immediately.

        This doesn't preserve order of the original channel, but allows things to
        be consumed as they are ready, which can provide better throughput depending
        on your workload.
        """
        return ReadyFutureChannel(self)


    def zip(self, *channels):
        """
        zip channel items together akin to build-in `zip` function.

        In the resulting `ZipChannel`, the items in `self` and all channels
        specified will be zipped together on a per-item basis.  The channel on
        which you're invoking `zip` will be the first, and items from the other
        channels will follow their order of specification in parameters.

        So...

            zipped = a.zip(b, c)

            yield zipped.next() --> (a0, b0, c0)
            yield zipped.next() --> (a1, b1, c1)
            ...

        """
        return ZipChannel([self] + list(channels))


    def cogroup(self, *channels):
        """
        cogroup channels of (key, value) items by keys ascending.

        Assuming `self` and all `channels` are structured with items of
        `(key, value)` pairs, and all emit items in ascending sort order of
        keys, emits the tuples of pairs across the channels such that we walk
        the total set of distinct keys in ascending order, and per channel,
        the pair with the greatest key less than or equal to the current key
        is emitted.

        See the `CoGroupChannel` for more.
        """
        return CoGroupChannel([self] + list(channels))


    def __getitem__(self, key_or_slice):
        """
        Access the `key_or_slice` of each item in the channel.

        In the resulting `MapChannel`, the `key_or_slice` of each item is
        propagated.

        For instance...

            hashchan = IterChannel(({"a": "A"}, {"a": "B"}))
            a_chan = hashchan["a"] # "A", "B"
        """
        return MapChannel(self, lambda item: item[key_or_slice])


    def windowby(self, keys_func=None):
        """
        Window the items according to `keys_func`.

        Assuming `keys_func` is a callable that, given as argument
        any item within `self`, returns a sequence of sortable, hashable
        keys, applies `keys_func` to each item and organizes items according
        to the window keys emitted.

        See the `WindowChannel` for more.
        """
        return WindowChannel(self, transform=keys_func)


    def groupby(self, key_func=None):
        """
        Group items according to `key_func`.

        Assuming `key_func` is a callable that, given as argument any
        item within `self`, returns grouping key, applies `key_func` to
        each item and organizes items into groups by grouping keys
        emitted.

        See the `GroupChannel` for more.
        """
        return GroupChannel(self, transform=key_func)


    def observe(self, observer):
        """
        Observe items on a channel for side effects

        Given a callable `observer`, passes each message on the channel
        to `observer` before passing the message along.  Allows observation
        of channel contents for purposes like logging, metrics, alerting,
        etc.

        See the `ObserveChannel` for more.
        """
        return ObserveChannel(self, observer)

    def chain(self, *channels):
        """
        Chain channels together into a single channel.

        Returns a :class:`ChainChannel` of the original channel chained with
        any number of given ``channels``.  See :class:`ChainChannel` for more.
        """
        return ChainChannel([self] + list(channels))


class ReadChannel(Channel):
    """
    Wraps any channel with a read-only interface.

    The contents of the messages of the `ReadChannel` will match
    that of the wrapped channel (including message order), but the
    `tornado.concurrent.Future` instances under the hood will not
    be the same as the wrapped channel.

    While this class can be used on its own (for instance, to make
    a `ProducerChannel` look like a read-only channel), its main purpose
    is as a base class for other implementations that apply specialized logic
    per-message.

    The wrapped source channel is available internally as `self.__channel__`.
    """
    def __init__(self, channel):
        super(ReadChannel, self).__init__(self.__reader__)
        self.__channel__ = channel


    @gen.coroutine
    def __reader__(self, thischan):
        """
        The data producer for a read-oriented channel.

        This starts the asynchronous consumption of the source channel.  It
        continues until the source channel is exhausted (ChannelDone exception).
        """
        head = self.__future__
        try:
            while not self.__done__:
                value = yield self.__next_item__()
                next_f = cc.Future()
                read_f = cc.Future()
                head.set_result((next_f, read_f, value))
                head = next_f
                # Block until something has read that last result.
                yield read_f
                yield gen.moment
        except ChannelDone:
            set_channel_done_exception(head, "ReadChannel.__reader__")
        except:
            compat.future_set_exc_info(head, sys.exc_info())

    @gen.coroutine
    def __next_item__(self):
        """
        Retrieves the next value from the source channel.

        This must return a `tornado.concurrent.Future` with either the next
        message or an exception.  This is easiest done via
        `@tornado.gen.coroutine`.
        """
        value = yield self.__channel__.next()
        raise gen.Return(value)


class MapChannel(ReadChannel):
    """
    A channel that applies a per-message transformation to its source channel.

    Initialized with a source channel and a transformation function, the function
    is applied to each source channel message and the result exposed as the
    output message.
    """

    def __init__(self, channel, transform):
        """
        Wraps `channel`, applying `transform` to each source message.

        The `transform` callable should be a traditional function; it's not
        expected to be asynchronous.  It's called per source message and the
        result is passed on as the published message.
        """
        super(MapChannel, self).__init__(channel)
        self.__transform__ = transform


    @gen.coroutine
    def __next_item__(self):
        value = yield self.__channel__.next()
        value = self.__transform__(value)
        raise gen.Return(value)


class ObserveChannel(MapChannel):
    """
    A channel allowing observation of values on a channel.

    Initialized with a source channel and an observer callable, the callable
    is applied to each source channel message; the result is discarded and the
    original message is passed along as the output message.

    This allows for side effects such as logging, metrics calculation,
    etc.
    """

    @classmethod
    def make_observer(cls, callable_):
        def observer(value):
            callable_(value)
            return value
        return observer

    def __init__(self, channel, observer):
        super(ObserveChannel, self).__init__(
                channel,
                self.make_observer(observer))


class FlatMapChannel(MapChannel):
    """
    A channel that allows a per-message transform to 0 or more output messages.

    Basically like `MapChannel`, in that a given transform function is
    applied to each message from a given source channel.  However, the transform
    result is expected to be iterable, and the items of each iterable are
    output as distinct messages to the downstream consumer.

    This allows for filtering (filter out items by returning the empty tuple),
    for exploding complex structures out into simpler sequences of values, etc.
    """

    @gen.coroutine
    def __reader__(self, thischan):
        head = self.__future__
        try:
            while not self.__done__:
                value = yield self.__next_item__()
                for subitem in value:
                    if self.__done__:
                        break
                    next_f = cc.Future()
                    read_f = cc.Future()
                    head.set_result((next_f, read_f, subitem))
                    head = next_f
                    # Block until something has read that item.
                    yield read_f
                    yield gen.moment

        except ChannelDone:
            set_channel_done_exception(head, "FlatMapChannel.__reader__")
        except:
            compat.future_set_exc_info(head, sys.exc_info())


class Windower(object):
    def __init__(self):
        self.members = {}
        self.keys = []

    def window(self, keys, value):
        mem = self.members
        active = self.keys

        # Unique keys per item
        keys = iter(sorted(set(keys)))
        i = 0

        try:
            while True:
                key = next(keys)
                while active[i] < key:
                    # Actives are sorted, keys are sorted; any active that is
                    # less than key must be emitted and cleared out.
                    k = active.pop(i)
                    yield k, mem.pop(k)

                if active[i] == key:
                    # The key is present in new list, so add item to members
                    # and keep the key active.
                    mem[key].append(value)
                    i += 1
                else:
                    # The key is new to the list, so add it.
                    active.insert(i, key)
                    mem[key] = [value]
                    i += 1

        except IndexError:
            # This occurs if there's nothing left in active keys.
            # Anything remaining in new keys gets collected.
            try:
                while True:
                    active.append(key)
                    mem[key] = [value]
                    key = next(keys)
            except StopIteration:
                # All done
                pass

        except StopIteration:
            # This occurs if we've used up all the input keys.  Anything
            # remaining is no longer active and should be emitted.
            while i < len(active):
                k = active.pop(i)
                yield k, mem.pop(k)


    def tail(self):
        active = self.keys
        mem = self.members
        while active:
            k = active.pop(0)
            yield k, mem.pop(k)


class WindowChannel(FlatMapChannel):
    """
    Groups input channel items into windows by key

    Given some input :class:`flowz.channel.core.Channel` and a
    :func:`transform` function, groups values from the input channel
    into windows based on the window keys returned by `transform`.

    Parameters:
        channel (:class:`flowz.channel.core.Channel`): the input channel
            from which input values are pulled

        function (callable): [OPTIONAL] a function that is called per input
            channel item (with that item as the sole argument), which should
            return an iterable collection of sortable, hashable window keys.

    When no function is given, each item is assumed to be a sequence with
    the keys in the first position (``item[0]``).

    Values from the `channel` will be gathered into windows according to
    the keys emitted per value by the `transform`; per value, if a
    previously-produced window key is no longer present, that window key
    and all its gathered values will be released by the ``WindowChannel``,
    as a ``(windowkey, valueslist)`` tuple.  Windows are released in
    sorted key order, and the values within each ``valueslist`` are in
    input-channel order (the order in which they were emitted by the input
    channel).

    An example::
        from __future__ import print_function

        # tuples as window keys, where the first item of the tuple indicates
        def groups(i):
            yield 'by2:%d' % (i // 2)
            yield 'by3:%d' % (i // 3)

        inputchan = IterChannel(range(5))
        windows = WindowChannel(inputchan, groups)

        Flo([windows.map(print)]).run()
        # Prints:
        # ('by2:0', [0, 1])
        # ('by3:0', [0, 1, 2])
        # ('by2:1', [2, 3])
        # ('by2:2', [4])
        # ('by3:1', [3, 4])

    The scope of a window key is bound to the sequence of items on the input
    channel; a window key and its associated values are emitted as soon as
    an item is encountered that does not produce that window key.  Thus, you
    may use a windowing scheme that repeats window keys across the domain of
    your input channel, but if those repeats are not sequential, you will
    get multiple windows with the same key.
    """

    def __init__(self, channel, transform=None):
        self.windower = self.get_windower()
        if transform is None:
            transform = self.default_transform
        super(WindowChannel, self).__init__(channel, transform)


    @staticmethod
    def default_transform(value):
        """
        Assumes ``value`` has window keys in its first position (value[0])
        """
        return value[0]


    @classmethod
    def get_windower(cls):
        return Windower()


    @gen.coroutine
    def __next_item__(self):
        try:
            value = yield self.__channel__.next()
            keys = self.__transform__(value)
            raise gen.Return(self.windower.window(keys, value))
        except ChannelDone:
            # We have to deal with the windower's tail after
            # the input channel is done.
            win = self.windower
            # If it's none, we've already been here before; this
            # channel is done (tail has been released)
            if win is None:
                raise ChannelDone("Channel is done (WindowChannel.__next_item__)")
            # If here, this is the first time the input channel through
            # the ChannelDone, so we release the windower's tail and
            # clear the windower from the channel.
            self.windower = None
            raise gen.Return(win.tail())


class Grouper(object):
    keyed = False

    def window(self, key, value):
        if self.keyed:
            if self.last_key == key:
                # Same key, so just accumulate and move on.
                self.values.append(value)
            else:
                # Different key, so release previous and start new.
                yield self.last_key, self.values
                self.last_key, self.values = key, [value]
        else:
            # First pass through
            self.keyed = True
            self.last_key, self.values = key, [value]


    def tail(self):
        if self.keyed:
            # If anything remains, send it off.
            yield self.last_key, self.values


class GroupChannel(WindowChannel):
    """
    Group items from an input channel based on keys from a function.

    Like :func:`itertools.groupby` from the standard library, but for channels;
    also like :class:`WindowChannel`, but the ``transform`` function should
    return a single hashable key, rather than a sequence of window keys.

    If no ``transform`` function is given, the default behavior is to treat
    each item as a sequence, with the desired key in first position
    (``item[0]``).
    """

    @classmethod
    def get_windower(cls):
        return Grouper()


class FilterChannel(FlatMapChannel):
    """
    Filters a source channel, passing through items that pass a predicate test.

    Given some input channel `channel`, and some predicate test callable
    `predicate`, the `FilterChannel` will consume `channel` and only emit
    items for which `predicate(item)` is `True`.
    """

    def __init__(self, channel, predicate):
        # This deliberately accesses MapChannel's super
        super(MapChannel, self).__init__(channel)
        self.__transform__ = self.make_transform(predicate)


    @classmethod
    def make_transform(cls, predicate):
        return lambda x: (x,) if predicate(x) else ()


class FutureChannel(ReadChannel):
    """
    Wraps a source channel that produces Futures, and waits on each message.

    Given a source channel whose messages may be `tornado.concurrent.Future`
    instances, the `FutureChannel` will wait for each such message to be done.
    The output message will be the result of the future.

    This can handle the case where not all the source messages are futures.

    The messages pass through in the same order as in the source channel, which
    means that a high-latency early source message will block the passage of
    a low-latency later source message, even though the lower-latency message may
    be ready first.
    """

    @gen.coroutine
    def __next_item__(self):
        value = yield self.__channel__.next()
        value = yield gen.maybe_future(value)
        raise gen.Return(value)


class ReadyFutureChannel(ReadChannel):
    """
    Wraps a source channel that produces Futures, passing results as they're ready.

    Just like the `FutureChannel`, except source message results are published
    *as they are ready*, rather than in source message order.

    This should give better throughput for workloads of significant variation in
    latency, at the cost of giving up deterministic ordering.
    """

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
                read_f = cc.Future()
                head_old.set_result((head_new, read_f, f.result()))
                if self._read_done and not self._waiting:
                    set_channel_done_exception(self.__head__, "ReadyFutureChannel.__item_ready__")


    @gen.coroutine
    def __next_item__(self):
        value = yield self.__channel__.next()
        # Make sure it's a future to ease downstream use.
        raise gen.Return(gen.maybe_future(value))


class TeeChannel(Channel):
    """
    A channel with independent consumption of its source channel.

    Given a source channel, the `TeeChannel` will act as an independent copy
    of that source channel, with independent consumption of messages, as of
    the state of the source channel at the time of making the `TeeChannel`.

    Thus, given a new, unread channel, the `TeeChannel` would pass along all
    the same messages as the source channel.

    Given a partially read source channel, the `TeeChannel` would pass along
    all the messages as the source channel from that point forward.

    Once a `TeeChannel` is made, the order, rate, etc. of consumption of the
    source channel and the `TeeChannel` make no logical difference; they provide
    the same messages to their consumers.  The side-effect of this is memory
    management complexity: messages remain in memory until all extant channels
    are done with them.

    This facilitates use of channels in a dependency-based programming paradigm;
    by "teeing" some channel, multiple consumers can work off that channel without
    issue.
    """

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
    """
    A read/write channel.

    A `ProducerChannel` passes through messages that are written to it
    via its asynchronous `put` method.

    Given an asynchronous `starter` function to kick off whatever routine
    produces values, any number of writer routines may put values into the
    `ProducerChannel`, and they come through in write-order on the read
    interface (`next` method).

    When writing is done, the producer routine(s) must call the `close` method,
    after which no further messages may be written to the channel.

    Writing to a closed channel raises a `ChannelDone` exception, just like
    reading from an exhausted channel.

    The `starter` is not required to do anything in particular; writer routines
    that put messages to the `ProducerChannel` may be launched before the channel
    is activated; `put` calls will block until activation occurs.  However,
    deferring activation of the producers until a consumer uses the channel is
    more in keeping with dependency-based programming.
    """
    def __init__(self, starter):
        super(ProducerChannel, self).__init__(starter)
        # head is the next guy to write.
        self.__head__ = self.__future__
        # ready indicates that we're good to go.
        self.__ready__ = cc.Future()


    @gen.coroutine
    def start(self):
        """
        Activates the channel for reading and writing.

        This behaves like the `start` of any `Channel`, but additionally, any
        would-be writers waiting on `put` calls will be unblocked.
        """
        if not self.__started__:
            self.__ready__.set_result(True)
            super(ProducerChannel, self).start()
        raise gen.Return(True)


    @gen.coroutine
    def put(self, item, exception=False):
        """
        Asynchronously place `item` onto the channel.

        Returns a `tornado.concurrent.Future` that will "block" until the
        channel is ready and the item has actually been put into the channel.

        Raises `ChannelDone` if the channel has already been closed.
        """
        yield self.__ready__
        last_f = self.__head__
        if last_f.done():
            raise ChannelDone("Channel is done (ProducerChannel.put)")
        if exception:
            last_f.set_exception(item)
        else:
            next_f = cc.Future()
            self.__head__ = next_f
            last_f.set_result((next_f, cc.Future(), item))
        raise gen.Return(True)


    def close(self):
        """
        Closes the channel for writing.

        This may be called any number of times, but once called, no further
        put operations are allowed.

        Note that this is distinct from putting the channel into a "done"
        state; the channel isn't "done" until reads are exhausted, just like
        any other channel.
        """
        if not self.__head__.done():
            set_channel_done_exception(self.__head__, "ProducerChannel.close")


class IterChannel(ProducerChannel):
    """
    Converts an iterable into a channel.

    Given an iterable, passes the iterable's values through the channel as
    messages.
    """

    def __init__(self, iterable):
        super(IterChannel, self).__init__(self.get_starter(iterable))

    @classmethod
    def get_starter(cls, iterable):
        @gen.coroutine
        def starter(chan):
            yield chan.__ready__
            try:
                for value in iterable:
                    yield chan.put(value)
            except ChannelDone:
                pass
            except Exception as e:
                yield chan.put(e, exception=True)
            else:
                chan.close()
        return starter


class ZipChannel(ReadChannel):
    """
    Zips multiple channels together for iterating as a group.

    Given N source channels, the ZipChannel acts like a channel-oriented
    `zip()`, such that:

        yield zc.next() --> (chan0.next(), chan1.next(), ... chanN.next())

    The `ZipChannel` produces `ChannelDone` and is considered exhausted as soon
    as any of its source channels reach that state.
    """
    def __init__(self, channels):
        # This deliberately accesses ReadChannel's super
        super(ReadChannel, self).__init__(self.__reader__)
        self.__channels__ = channels


    @gen.coroutine
    def __next_item__(self):
        r = []
        # Less efficient than yielding the list of futures,
        # but quieter when exceptions are thrown.
        for i, c in enumerate(self.__channels__):
            v = yield c.next()
            r.append(v)
        raise gen.Return(tuple(r))


class ChainChannel(ReadChannel):
    """
    Chains multiple channels together for iterating in sequence.

    Given N source channels, the ChainChannel acts like a channel-oriented
    :func:`itertools.chain`, such that:

        yield zc.next() --> chan0.next()
        yield zc.next() --> chan0.next() # raises ChannelDone! --> chan1.next()
        yield zc.next() --> chan1.next()
        yield zc.next() --> chan1.next() # raises ChannelDone! --> chan2.next()
        ...
        yield zc.next() --> chanN.next()
        yield zc.next() --> chanN.next() # raises ChannelDone!

    The ``ChainChannel`` produces ``ChannelDone`` and is considered exhausted
    one the final channel in its input channels raises ``ChannelDone``.
    """
    def __init__(self, channels):
        # The use of ReadChannel's super is deliberate, as ReadChannel
        # assumes a single input channel.
        self.__channels__ = list(channels)
        super(ReadChannel, self).__init__(self.__reader__)


    @gen.coroutine
    def __next_item__(self):
        chn = self.__channels__
        while chn:
            try:
                v = yield chn[0].next()
                raise gen.Return(v)
            except ChannelDone:
                chn.pop(0)
        raise ChannelDone("Channel is done (ChainChannel.__next_item__)")




class CoGroupChannel(ReadChannel):
    """
    Walks all input channels in ascending key order

    Given some number of channels, each of which emits items as (k, v) pairs,
    where all k are sorted in ascending order, walks the channels in order,
    such that each distinct k is seen.  For each successive k, the (k, v) pair
    from each channel will be the greatest k seen that is less than or equal to
    the current k.


    Suppose that we have two channels:

    ```
        S             D
     ------         ----
    (K0, S0)      (K0, D0)
    (K1, S1)      (K1, D1)
    (K3, S3)      (K2, D2)
    (K4, S4)

    ```

    In the cogrouped world, we would get this as:

    ```
    ((K0, S0), (K0, D0))
    ((K1, S1), (K1, D1))
    ((K1, S1), (K2, D2))
    ((K3, S3), (K2, D2))
    ((K4, S4), (K2, D2))
    ```

    In other words, we walk the ordered set of all keys _SKeys_ + _DKeys_,
    and for each such key, we should the most recent `(key, value)` pair per input
    channel less than or equal to the current key.

    This can be done with any number of input channels, as long as they follow the
    key/sorting assumptions.

    If one of the input channels has a first key that is higher than the others,
    it will appear as `None` until its key is active.  Suppose, we add a third channel,
    to see.

    ```
        X
     ------
    (K2, X2)
    (K3, X3)
    (K4, X4)
    ```

    If we cogroup S, D, and X, we get:

    ```
    ((K0, S0), (K0, D0), None)
    ((K1, S1), (K1, D1), None)
    ((K1, S1), (K2, D2), (K2, X2))
    ((K3, S3), (K2, D2), (K3, X3))
    ((K4, S4), (K2, D2), (K4, X4))
    ```
    """

    _last_key = util.MINIMUM
    _max_key = util.MAXIMUM

    def __init__(self, channels):
        super(ReadChannel, self).__init__(self.__reader__)
        self.channels = channels
        self._states = [None for _ in range(len(channels))]

    @gen.coroutine
    def __reader__(self, thischan):
        r = None
        self._futures = [chan.next() for chan in self.channels]
        r = yield super(CoGroupChannel, self).__reader__(self)
        raise gen.Return(r)

    @gen.coroutine
    def __next_item__(self):
        pairs = []
        next_reads = []

        # Note that to ensure MAXIMUM and MINIMUM control comparisons with
        # new keys, comparisons should always use last_key/next_key as the
        # first operator of the comparison operator.
        last_key = self._last_key
        next_key = self._max_key

        # Get the next states of the futures.
        for i, future in enumerate(list(self._futures)):
            try:
                pair = yield future
                # Must be greater than last key to be a candidate.
                if last_key < pair[0]:
                    # Less than means new candidate key.
                    # Must be less than next key to be candidate key
                    if next_key > pair[0]:
                        next_reads[:] = [(i, pair)]
                        next_key = pair[0]
                    # Equal means to include alongside other candidates.
                    elif next_key == pair[0]:
                        next_reads.append((i, pair))
            except ChannelDone:
                # Release reference to channel
                self.channels[i] = None

        # If there are no next_reads, there is no more work to be done.
        if not next_reads:
            # No qualified keys remaining.  We're done.
            raise ChannelDone("Channel is done (CoGroupChannel.__next_item__)")

        # Propagates the guys with the lowest qualifying key to the state
        # list, and asynchronously fetch their respective channel's next vals.
        for i, pair in next_reads:
            self._states[i] = pair
            chan = self.channels[i]
            if chan:
                self._futures[i] = chan.next()

        self._last_key = next_key
        raise gen.Return(tuple(self._states))
