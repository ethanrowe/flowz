import datetime

import mock
from nose import tools
from tornado import gen
from tornado import testing as tt

from flowz import channels
from .util import raises_channel_done


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


class TransformingChannelTest(ChannelTest):
    """
    Like the baseline `ChannelTest` except implementations provide
    a value conversion function, when we expect the values emitted by
    the channel to differ from that of the input channel.
    """
    
    def determine_expected_values(self, values):
        """
        Given the input `values` returns the expected emitted values.
        """
        return values

    def verify_channel_values(self, chan, values, nesting=False):
        if not hasattr(self, '_mapped_values'):
            self._mapped_values = self.determine_expected_values(values)
            values = self._mapped_values
        return super(TransformingChannelTest, self).verify_channel_values(
                chan, values, nesting=nesting)



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


class WindowChannelTest(ChannelTest, tt.AsyncTestCase):
    _prepared = None

    def get_sortable_key(self, i):
        k = mock.Mock(name='SortableKey%d' % i)
        k.key = i
        k.__eq__ = lambda m, o: i == getattr(o, 'key', None)
        k.__ne__ = lambda m, o: i != getattr(o, 'key', None)
        k.__lt__ = lambda m, o: i < getattr(o, 'key', None)
        k.__le__ = lambda m, o: i <= getattr(o, 'key', None)
        k.__gt__ = lambda m, o: i > getattr(o, 'key', None)
        k.__ge__ = lambda m, o: i >= getattr(o, 'key', None)
        k.__hash__ = lambda m: hash(i)
        return k

    def get_sortable_keys(self, count):
        for i in range(count):
            yield self.get_sortable_key(i)

    def prepared(self, values):
        if self._prepared is None:
            self._prepared = self.prepare_keys_and_values(values)
        return self._prepared

    def prepare_keys_and_values(self, values):
        # Each key (but the last) will get emitted with
        # two items in sequence.
        # So we'll expect a pair of items per key.
        keys = list(self.get_sortable_keys(len(values)))
        keys_by_value = dict((v, [k]) for v, k in zip(values, keys))
        values_by_key = dict((k, [v]) for k, v in zip(keys, values))
        for v, k in zip(values[1:], keys):
            keys_by_value[v].append(k)
            values_by_key[k].append(v)
        return keys_by_value, values_by_key

    def determine_expected_values(self, values):
        _, by_key = self.prepared(values)
        return [(k, list(by_key[k]))
                for k in sorted(by_key.keys())]

    @gen.coroutine
    def verify_channel_values(self, chan, values, nesting=False):
        if not hasattr(self, '_original_values'):
            self._original_values = values
        if values == self._original_values:
            values = self.determine_expected_values(values)
        # If we've already mapped them, we expect `values` to already be
        # expressed in terms of our expectation.
        yield super(WindowChannelTest, self).verify_channel_values(
                chan, values, nesting=nesting)


    def get_channel_with_values(self, values):
        by_val, _ = self.prepared(values)
        c = channels.IterChannel(iter(values))
        # Per input value, returns the sequence of associated keys.
        return channels.WindowChannel(c, lambda v: iter(by_val[v]))


class WindowChannelDuplicateKeysTest(WindowChannelTest):
    def get_channel_with_values(self, values):
        by_val, _ = self.prepared(values)
        c = channels.IterChannel(iter(values))
        # Per input value, we double the sequence of associated keys.
        return channels.WindowChannel(c, lambda v: iter(by_val[v] + by_val[v]))


class WindowChannelNumericKeysTest(WindowChannelTest):
    def alt_values(self):
        return list(range(5))

    def get_channel_with_values(self, values):
        return super(WindowChannelNumericKeysTest, self).get_channel_with_values(
                self.alt_values())

    def prepare_keys_and_values(self, values):
        # We emit div 2 and div 3 as keys per item.
        # This means we'll get some messy duplicates.
        keys_by_value = dict(
                (i, [i // x for x in (2, 3)])
                for i in self.alt_values())
        # Table of values to emitted keys,
        # with arrows indicating the point at which keys are emitted.
        # We dedup keys per value, so parens show the deduped keys
        # 0: 0, 0 (0)
        # 1: 0, 0 (0)
        # 2: 1, 0 (1, 0)
        # 3: 1, 1 (1) -> 0: 0, 1, 2
        # 4: 2, 1 (2, 1)
        #  -> 1: 2, 3, 4
        #  -> 2: 4
        vals_by_key = {
                0: [0, 1, 2],
                1: [2, 3, 4],
                2: [4]}
        return keys_by_value, vals_by_key



class WindowWithNoFunc(object):
    def item_getter(self, keys):
        return lambda _, i: (list(keys),)[i]

    def get_channel_with_values(self, values):
        # In this case, no transform function is given.
        # The default behavior should be to attempt to
        # take the first positional element of each value.
        by_val, _ = self.prepared(values)
        for val in values:
            val.__getitem__ = self.item_getter(by_val[val])
        c = channels.IterChannel(iter(values))
        return channels.WindowChannel(c)


class WindowChannelDefaultTransformTest(WindowWithNoFunc, WindowChannelTest):
    pass


class WindowChannelSortingTest(WindowChannelTest):
    def get_channel_with_values(self, values):
        by_val, _ = self.prepared(values)
        c = channels.IterChannel(iter(values))
        # This reverses the order of keys emitted per item, but we expect
        # that those window keys will still be emitted in sorted order on
        # the other side.
        return channels.WindowChannel(c, lambda v: reversed(by_val[v]))


class WindowChannelSortingDefaultTransformTest(
        WindowWithNoFunc, WindowChannelSortingTest):
    def item_getter(self, keys):
        return super(
                WindowChannelSortingDefaultTransformTest, self).item_getter(
                        list(reversed(keys)))


class WindowChannelNoKeysTest(WindowChannelTest):
    def prepare_keys_and_values(self, values):
        key = self.get_sortable_key(0)
        # Every even value emits key
        # Every odd value emits no key
        # The odd values won't be included in anything.
        # The even values will get one window each because
        # the key wasn't sustained across consecutive values,
        # resulting in distinct windows.
        keys_by_value = dict((v, [key]) for v in values[slice(0, None, 2)])
        keys_by_value.update(dict((v, []) for v in values[slice(1, None, 2)]))
        return keys_by_value, key

    def determine_expected_values(self, values):
        _, key = self.prepared(values)
        return [(key, [val]) for val in values[slice(0, None, 2)]]


class WindowChannelNoKeysDefaultTransformTest(
        WindowWithNoFunc, WindowChannelNoKeysTest):
    pass

class WindowChannelCommonValueTest(WindowChannelTest):
    def prepare_keys_and_values(self, values):
        # One key will be common to everything, while
        # other keys will be per-item.
        keys = list(self.get_sortable_keys(len(values) + 1))
        # We expect the last guy to be associated with all of 'em.
        keys_by_value = dict((v, [k, keys[-1]]) for v, k in zip(values, keys))
        values_by_key = dict((k, [v]) for k, v in zip(keys, values))
        # And again, the last key is associated with all values.
        values_by_key[keys[-1]] = list(values)
        return keys_by_value, values_by_key

class WindowChannelCommonValueDefaultTransformTest(
        WindowWithNoFunc, WindowChannelCommonValueTest):
    pass

class WindowChannelCommonValueEarlySortTest(WindowChannelTest):
    def prepare_keys_and_values(self, values):
        # The earliest key will be common to everything, while others
        # are per-item.
        keys = list(self.get_sortable_keys(len(values) + 1))
        keys_by_value = dict((v, [keys[0], k]) for v, k in zip(values, keys[1:]))
        values_by_key = dict((k, [v]) for k, v in zip(keys[1:], values))
        values_by_key[keys[0]] = list(values)
        return keys_by_value, values_by_key

    def determine_expected_values(self, values):
        _, values_by_key = self.prepared(values)
        keys = list(sorted(values_by_key.keys()))
        key = keys.pop(0)
        keys.insert(-1, key)
        return [(k, values_by_key[k]) for k in keys]

class WindowChannelCommonValueEarlySortNoTransformTest(
        WindowWithNoFunc, WindowChannelCommonValueEarlySortTest):
    pass

class GroupChannelTest(WindowChannelTest):
    def prepare_keys_and_values(self, values):
        # Must return tuple of:
        # - a dict mapping value to associated group by
        # - the actual list of expected channel values.
        # For the base case, we'll just do simplistic windowing
        # based on buckets of two.
        key_by_value = dict(
                (v, i // 2) for i, v in enumerate(values))
        expected_vals = [
                (i // 2, values[i:i+2])
                for i in range(0, len(values), 2)]
        return key_by_value, expected_vals

    def determine_expected_values(self, values):
        _, expect_vals = self.prepared(values)
        return expect_vals

    def get_channel_with_values(self, values):
        by_val, _ = self.prepared(values)
        c = channels.IterChannel(iter(values))
        # Per input value, returns the single associated key
        return channels.GroupChannel(c, lambda v: by_val[v])


class GroupChannelDefaultTransformTest(GroupChannelTest):
    def item_getter(self, item):
        return lambda _, i: (item,)[i]

    def get_channel_with_values(self, values):
        # In this case, no transform function is given; the
        # default behavior is to assume the grouping key is
        # the first positional element of each value (val[0]).
        by_val, _ = self.prepared(values)
        for val in values:
            val.__getitem__ = self.item_getter(by_val[val])
        c = channels.IterChannel(iter(values))
        # No transform functon.
        return channels.GroupChannel(c)


class GroupChannelRepeatKeysTest(GroupChannelTest):
    def prepare_keys_and_values(self, values):
        # Group using mod 2, so we get repeat keys.
        key_by_value = dict(
                (v, i % 2) for i, v in enumerate(values))
        vals = [(i % 2, [v]) for i, v in enumerate(values)]
        return key_by_value, vals


class GroupChannelOneKeyTest(GroupChannelTest):
    def prepare_keys_and_values(self, values):
        key = mock.Mock(name='AMockKey')
        key_by_value = dict((v, key) for v in values)
        return key_by_value, [(key, list(values))]


class ChannelObserveTest(ChannelTest, ChannelExceptionTest, tt.AsyncTestCase):
    @property
    def observer(self):
        if not hasattr(self, '_observer'):
            self._observer = mock.Mock(name='ObservationCallable')
        return self._observer

    def get_channel_with_values(self, values):
        c = channels.IterChannel(iter(values))
        return channels.ObserveChannel(c, self.observer)


    def get_channel_values_before_error(self, values):
        def vals():
            for v in values:
                yield v
            raise TestException("Boom!")

        c = channels.IterChannel(vals())
        return channels.ObserveChannel(c, self.observer)


    @gen.coroutine
    def verify_channel_values(self, chan, values, nesting=False):
        if not hasattr(self, '_values'):
            self._values = values

        r = yield super(ChannelObserveTest, self).verify_channel_values(
                chan, values, nesting=nesting)

        # The observer should be called exactly once per input value, from
        # the *original* values, not from teeing or any such thing.
        tools.assert_equal(
                self.observer.call_args_list,
                [((val,), {}) for val in self._values])

        raise gen.Return(r)


    @tt.gen_test
    def test_observer_exception(self):
        # Our observer throws an exception.
        def observer(val):
            raise TestException(self, val)

        vals = [mock.Mock() for i in range(3)]
        chan = channels.IterChannel(iter(vals))
        chan = channels.ObserveChannel(chan, observer)

        try:
            # We expect the observer's exception to propagate to here.
            yield chan.next()
            # If we got this far, the exception was swallowed up.
            raise Exception("Failed to raise observer exception!")
        except TestException as e:
            # We confirmed the type match by getting here, so just confirm
            # the args to be confident it's the right exception.
            tools.assert_equal((self, vals[0]), e.args)

class ChainChannelTest(ChannelTest, ChannelExceptionTest, tt.AsyncTestCase):
    def split_values(self, values):
        # Two channels together.
        split = (len(values) // 2) + 1
        va, vb = values[:split], values[split:]
        return va, vb

    def get_channel_with_values(self, values):
        chns = [channels.IterChannel(vals)
                for vals in self.split_values(values)]
        return channels.ChainChannel(chns)

    def get_channel_values_before_error(self, values):
        def valgen(vals):
            for v in vals:
                yield v
            raise TestException("Boom!")

        splits = list(self.split_values(values))
        splits[-1] = valgen(splits[-1])
        chans = [channels.IterChannel(vls)
                 for vls in splits]
        return channels.ChainChannel(chans)


class ChainChannelWithEmptiesTest(ChainChannelTest):
    def split_values(self, values):
        # An empty channel between every other one-channel value
        for value in values:
            yield [value]
            yield ()


class ChainChannelRearLoadedTest(ChainChannelTest):
    def split_values(self, values):
        # All the values are in the final of three channels.
        yield ()
        yield ()
        yield values

class ChainChannelFrontLoadedTest(ChainChannelTest):
    def split_values(self, values):
        # All the values are in the first of three channels.
        yield values
        yield ()
        yield ()

