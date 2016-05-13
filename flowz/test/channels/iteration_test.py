import datetime
import itertools

import mock
from nose import tools
from tornado import gen
from tornado import testing as tt

from flowz import channels

@gen.coroutine
def raises_channel_done(channel):
    try:
        yield channel.next()
        raise Exception("ChannelDone not raised!")
    except channels.ChannelDone:
        # This just increments the test count,
        tools.assert_equal(True, True)


@gen.coroutine
def sleeper(val, t):
    yield gen.sleep(t)
    raise gen.Return(val)


@gen.coroutine
def waiter(val, depends):
    if depends:
        yield depends
    yield gen.sleep(0.01)
    raise gen.Return(val)


class TestException(Exception):
    pass

class ChannelTest(object):
    """
    Baseline channel test that exercises behaviors common to
    any channel implementation.
    """
    @tt.gen_test
    def test_basic_iteration(self):
        values = [mock.Mock() for _ in range(5)]
        chan = self.get_channel_with_values(values)

        yield self.verify_channel_values(chan, values)


    @gen.coroutine
    def verify_channel_values(self, chan, values, nesting=False):
        kids = []

        # Don't care whether its literally False or None
        tools.assert_equal(False, chan.done() or False)
        for i, val in enumerate(values):
            received = yield chan.next()
            tools.assert_equal(val, received)
            # Don't care whether its literally False or None
            tools.assert_equal(False, chan.done() or False)

            if nesting:
                kids.append(self.verify_channel_values(
                    channels.TeeChannel(
                        chan), values[i + 1:], nesting=nesting))

        yield raises_channel_done(chan)

        tools.assert_equal(True, chan.done())

        if kids:
            yield kids


    @tt.gen_test
    def test_chan_before_teeing(self):
        values = [mock.Mock() for _ in range(5)]
        chan = self.get_channel_with_values(values)

        tee = channels.TeeChannel(chan)
        tee_of_tee = channels.TeeChannel(tee)

        yield self.verify_channel_values(chan, values)
        yield self.verify_channel_values(tee, values)
        yield self.verify_channel_values(tee_of_tee, values)


    @tt.gen_test
    def test_teeing_before_chan(self):
        values = [mock.Mock() for _ in range(5)]
        chan = self.get_channel_with_values(values)

        tee = channels.TeeChannel(chan)
        tee_of_tee = channels.TeeChannel(tee)

        yield self.verify_channel_values(tee, values)
        yield self.verify_channel_values(chan, values)
        yield self.verify_channel_values(tee_of_tee, values)


    @tt.gen_test
    def test_tee_of_tee_before_chan(self):
        values = [mock.Mock() for _ in range(5)]
        chan = self.get_channel_with_values(values)

        tee = channels.TeeChannel(chan)
        tee_of_tee = channels.TeeChannel(tee)

        yield self.verify_channel_values(tee_of_tee, values)
        yield self.verify_channel_values(chan, values)
        yield self.verify_channel_values(tee, values)


    @tt.gen_test
    def test_nested_tee_iteration(self):
        values = [mock.Mock(name='Iteration%d' % i) for i in range(5)]
        chan = self.get_channel_with_values(values)

        # This guy ensures that if you tee a partially-consumed channel,
        # the resulting tee only sees the items remaining.  It further
        # ensures it recursively down the graph of futures.
        yield self.verify_channel_values(chan, values, nesting=True)


class ChannelExceptionTest(object):
    @tt.gen_test
    def test_iteration_and_exception(self):
        vals = [mock.Mock() for _ in range(3)]
        chan = self.get_channel_values_before_error(vals)

        yield self.verify_channel_values_and_error(chan, vals)


    @gen.coroutine
    def verify_channel_values_and_error(self, chan, values):
        tools.assert_equal(False, chan.done() or False)
        for i, val in enumerate(values):
            received = yield chan.next()
            tools.assert_equal(val, received)
            tools.assert_equal(False, chan.done() or False)

        # Do this twice because the state should be basically stuck.
        try:
            yield chan.next()
        except TestException:
            pass

        tools.assert_equal(False, chan.done() or False)

        try:
            yield chan.next()
        except TestException:
            pass

        tools.assert_equal(False, chan.done() or False)


class IterChannelTest(ChannelTest, ChannelExceptionTest, tt.AsyncTestCase):
    def get_channel_with_values(self, values):
        return channels.IterChannel(iter(values))


    def get_channel_values_before_error(self, values):
        def valgen():
            for v in values:
                yield v
            raise TestException("Boom!")
        return channels.IterChannel(valgen())


class ReadChannelTest(IterChannelTest):
    def get_channel_with_values(self, values):
        c = super(ReadChannelTest, self).get_channel_with_values(values)
        return channels.ReadChannel(c)

    def get_channel_values_before_error(self, values):
        c = super(ReadChannelTest, self).get_channel_values_before_error(values)
        return channels.ReadChannel(c)


class ProducerChannelTest(ChannelTest, tt.AsyncTestCase):
    def producer(self, values):
        @gen.coroutine
        def _producer(chan):
            yield gen.sleep(0.01)
            for val in values:
                yield chan.put(val)
                yield gen.sleep(0.01)
            chan.close()
        return _producer


    def get_channel_with_values(self, values):
        return channels.ProducerChannel(self.producer(values))


class FutureChannelTest(ChannelTest, tt.AsyncTestCase):
    def get_channel_with_values(self, values):
        # We mix futures and regular values because it should
        # handle both.
        c = channels.IterChannel(
                sleeper(v, (i+1) * 0.01) if i % 2 == 0 else v
                for i, v in enumerate(values))
        return channels.FutureChannel(c)


class ReadyFutureChannelTest(ChannelTest, tt.AsyncTestCase):
    _first_nest = True

    def ordered_values(self, values):
        """
        Imposes a consistent order on values differing from the original
        """
        odds = list(reversed(values[1::2]))
        evens = list(values[::2])
        return odds + evens

    def get_channel_with_values(self, values):
        futures = {}
        for val in self.ordered_values(values):
            futures[id(val)] = waiter(val, list(futures.values()))

        c = channels.IterChannel(futures[id(val)] for val in values)
        return channels.ReadyFutureChannel(c)


    @gen.coroutine
    def verify_channel_values(self, chan, values, nesting=False):
        # This sucks but should work.
        first = False
        if self._first_nest:
            first = True
            self._first_nest = False
            values = self.ordered_values(values)
        try:
            r = yield super(ReadyFutureChannelTest, self).verify_channel_values(
                chan, values, nesting=nesting)
            raise gen.Return(r)
        finally:
            if first:
                self._first_nest = True


class ZipChannelWithTwoTest(ChannelTest, tt.AsyncTestCase):
    CHANNELS = 2

    def expanded_values(self, values):
        return [tuple(id(val) // (i + 1) for i in range(self.CHANNELS))
                for val in values]

    def get_channel_with_values(self, values):
        values = self.expanded_values(values)
        chans = [channels.IterChannel([v[i] for v in values])
                for i in range(self.CHANNELS)]

        return channels.ZipChannel(chans)

    @gen.coroutine
    def verify_channel_values(self, chan, values, nesting=False):
        if values and not isinstance(values[0], tuple):
            values = self.expanded_values(values)
        r = yield super(ZipChannelWithTwoTest, self).verify_channel_values(
                chan, values, nesting=nesting)
        raise gen.Return(r)


    @tt.gen_test
    def shortest_channel_determines_length_test(self):
        lengths = [5 + i for i in range(self.CHANNELS)]
        all_values = [[mock.Mock(name='Length%dValue%d' % (l, i))
                        for i in range(l)]
                        for l in lengths]

        chans = [channels.IterChannel(v) for v in all_values]

        expect = list(zip(*all_values))
        chan = channels.ZipChannel(chans)

        yield self.verify_channel_values(chan, expect)


class ZipChannelWithTenTest(ZipChannelWithTwoTest):
    CHANNELS = 10


class MapChannelTest(ChannelTest, ChannelExceptionTest, tt.AsyncTestCase):
    def expected_values(self, values):
        if not hasattr(self, '_expected_vals'):
            self._expected_vals = dict(
                    (val, val.transform.return_value)
                    for val in values)
            for val in list(self._expected_vals.values()):
                self._expected_vals[val] = val
        return [self._expected_vals[val] for val in values]


    def get_channel_with_values(self, values):
        c = channels.IterChannel(values)
        return channels.MapChannel(c, lambda val: val.transform())


    def get_channel_values_before_error(self, values):
        valmap = dict((value, value.transform.return_value)
                for value in values)
        c = channels.IterChannel(values + [None])
        def mapper(value):
            try:
                return valmap[value]
            except KeyError:
                raise TestException("Boom!")

        return channels.MapChannel(c, mapper)


    def verify_channel_values_and_error(self, chan, values):
        return super(MapChannelTest, self).verify_channel_values_and_error(
                chan, self.expected_values(values))

    def verify_channel_values(self, chan, values, nesting=False):
        # This depends on the fact that expected_values is stateful,
        # which is admittedly a cruddy design.
        return super(MapChannelTest, self).verify_channel_values(
                chan, self.expected_values(values), nesting=nesting)


class FilterChannelTest(ChannelTest, tt.AsyncTestCase):
    def valset(self, values):
        if not hasattr(self, '_valset'):
            self._valset = set(
                    v for i, v in enumerate(values)
                    if i % 2 == 0)
        return self._valset


    def expected_values(self, values):
        return [v for v in values if v in self.valset(values)]


    def get_channel_with_values(self, values):
        vals = self.valset(values)
        c = channels.IterChannel(values)
        return channels.FilterChannel(c, lambda val: val in vals)


    def verify_channel_values(self, chan, values, nesting=False):
        return super(FilterChannelTest, self).verify_channel_values(
                chan, self.expected_values(values),
                nesting=nesting)


class FilterChannelReverseTest(FilterChannelTest):
    def valset(self, values):
        if not hasattr(self, '_valset'):
            self._valset = set(
                    v for i, v in enumerate(values)
                    if i % 2 == 1)
        return self._valset


class FlatMapChannelTest(ChannelTest, ChannelExceptionTest, tt.AsyncTestCase):
    INTERVAL = 2

    def intervalic_values(self, values):
        return [
            values[i:i + self.INTERVAL] if i % self.INTERVAL == 0 else ()
            for i, v in enumerate(values)]


    def build_mapper(self, values):
        """
        Return subslices of `values` according to `self.INTERVAL`.

        Any value that is positionally at an index of `values` that
        is divisible by `self.INTERVAL` will produce an iterator of
        `self.INTERVAL` values.

        Any other value will produce an empty iterator.
        """
        valmap = dict(zip(values, self.intervalic_values(values)))
        return lambda v: iter(valmap[v])

    
    def build_exception_mapper(self, values):
        mapper = self.build_mapper(values)
        def _mapper(v):
            try:
                return mapper(v)
            except KeyError:
                raise TestException("Blowing up!")
        return _mapper


    def get_channel_with_values(self, values):
        c = channels.IterChannel(values)
        return channels.FlatMapChannel(c, self.build_mapper(values))


    def get_channel_values_before_error(self, values):
        c = channels.IterChannel(values + [object()])
        return channels.FlatMapChannel(c, self.build_exception_mapper(values))


class FlatMapChannelByThreeTest(FlatMapChannelTest):
    INTERVAL = 3


class CoGroupInterleavedChannelTest(ChannelTest, tt.AsyncTestCase):
    _expected_vals = None

    def expected_values(self, values):
        if not self._expected_vals:
            self._expected_vals = self.determine_expected_values(values)
        return self._expected_vals[len(self._expected_vals) - len(values):]


    def determine_expected_values(self, values):
        # 3 channels, one has all vals, others have alternating.
        vals = []
        last = None
        for i, val in enumerate(values):
            vals.append((
                (i, val) if i % 2 == 0 else last,
                (i, val) if i % 2 == 1 else last,
                (i, val)))
            last = (i, val)
        return vals


    def reduce(self, iterable):
        i = iter(iterable)
        try:
            last = next(i)
            yield last
            while True:
                n = next(i)
                if n != last:
                    last = n
                    yield n
        except StopIteration:
            pass


    def get_channel_with_values(self, values):
        values = self.expected_values(values)
        numchans = len(values[0])
        chans = [list(self.reduce(
                    row[i] for row in values if row[i] is not None))
                for i in range(numchans)]
        chans = [channels.IterChannel(seq)
                 for seq in chans]
        return channels.CoGroupChannel(chans)


    def verify_channel_values(self, chan, values, nesting=False):
        return super(
                CoGroupInterleavedChannelTest, self).verify_channel_values(
                        chan, self.expected_values(values), nesting=nesting)


class CoGroupIdenticalKeysChannelTest(CoGroupInterleavedChannelTest):
    def determine_expected_values(self, values):
        values = list(values)
        return list(zip(
            enumerate(values[:]),
            enumerate(values[1:] + values[:1]),
            enumerate(values[2:] + values[:2])))


class CoGroupInterleavedTimeKeyChannelTest(CoGroupInterleavedChannelTest):
    def determine_expected_values(self, values):
        td = datetime.timedelta(days=1)
        base = datetime.datetime(2016, 2, 1)
        values = super(
                CoGroupInterleavedTimeKeyChannelTest,
                self).determine_expected_values(values)
        # Convert integer keys to datetimes.
        return [tuple((base + (kv[0] * td), kv[1])
                    if kv else kv for kv in row)
                for row in values]


class CoGroupStaggeredKeysChannelTest(CoGroupInterleavedChannelTest):
    def determine_expected_values(self, values):
        values = list(values)
        k = len(values) // 2
        a = list(enumerate(values[:k]))
        a += [a[-1] for _ in values[k:]]
        b = [None for _ in values[:k]]
        b += [(i + k, val) for i, val in enumerate(values[k:])]
        return list(zip(a, b))

