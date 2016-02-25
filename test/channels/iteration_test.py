import itertools

import mock
from nose import tools
from tornado import gen
from tornado import testing as tt

from flo import channels

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


class IterChannelTest(ChannelTest, tt.AsyncTestCase):
    def get_channel_with_values(self, values):
        return channels.IterChannel(iter(values))


class ReadChannelTest(ChannelTest, tt.AsyncTestCase):
    def get_channel_with_values(self, values):
        c = channels.IterChannel(iter(values))
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


