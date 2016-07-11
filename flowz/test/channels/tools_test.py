from nose import tools
from tornado import gen
from tornado import testing as tt

from flowz import channels
from flowz.channels.tools import rolling, pinned_group_size, exact_group_size
from .util import raises_channel_done


class RollingWindowTest(tt.AsyncTestCase):
    # verify_channel_values borrowed from iteration_test.ChannelTest
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
    def test_partials(self):
        values = [i for i in range(10)]
        chan = channels.IterChannel(iter(values)).windowby(rolling(5))
        yield self.verify_channel_values(chan,
                                         ((0, [0]),
                                          (1, [0, 1]),
                                          (2, [0, 1, 2]),
                                          (3, [0, 1, 2, 3]),
                                          (4, [0, 1, 2, 3, 4]),
                                          (5, [1, 2, 3, 4, 5]),
                                          (6, [2, 3, 4, 5, 6]),
                                          (7, [3, 4, 5, 6, 7]),
                                          (8, [4, 5, 6, 7, 8]),
                                          (9, [5, 6, 7, 8, 9]),
                                          (10, [6, 7, 8, 9]),
                                          (11, [7, 8, 9]),
                                          (12, [8, 9]),
                                          (13, [9])))

    @tt.gen_test
    def test_no_partials(self):
        values = [i for i in range(10)]
        chan = channels.IterChannel(iter(values)).windowby(rolling(5)).filter(exact_group_size(5))
        yield self.verify_channel_values(chan,
                                         ((4, [0, 1, 2, 3, 4]),
                                          (5, [1, 2, 3, 4, 5]),
                                          (6, [2, 3, 4, 5, 6]),
                                          (7, [3, 4, 5, 6, 7]),
                                          (8, [4, 5, 6, 7, 8]),
                                          (9, [5, 6, 7, 8, 9])))

    @tt.gen_test
    def test_pin_both(self):
        values = [i for i in range(10)]
        chan = channels.IterChannel(iter(values)).windowby(rolling(5)).filter(pinned_group_size(2, 4))
        yield self.verify_channel_values(chan,
                                         ((1, [0, 1]),
                                          (2, [0, 1, 2]),
                                          (3, [0, 1, 2, 3]),
                                          (10, [6, 7, 8, 9]),
                                          (11, [7, 8, 9]),
                                          (12, [8, 9])))

    @tt.gen_test
    def test_pin_upper(self):
        values = [i for i in range(10)]
        chan = channels.IterChannel(iter(values)).windowby(rolling(5)).filter(pinned_group_size(upper=4))
        yield self.verify_channel_values(chan,
                                         ((0, [0]),
                                          (1, [0, 1]),
                                          (2, [0, 1, 2]),
                                          (3, [0, 1, 2, 3]),
                                          (10, [6, 7, 8, 9]),
                                          (11, [7, 8, 9]),
                                          (12, [8, 9]),
                                          (13, [9])))

    @tt.gen_test
    def test_pin_lower(self):
        values = [i for i in range(10)]
        chan = channels.IterChannel(iter(values)).windowby(rolling(5)).filter(pinned_group_size(lower=4))
        yield self.verify_channel_values(chan,
                                         ((3, [0, 1, 2, 3]),
                                          (4, [0, 1, 2, 3, 4]),
                                          (5, [1, 2, 3, 4, 5]),
                                          (6, [2, 3, 4, 5, 6]),
                                          (7, [3, 4, 5, 6, 7]),
                                          (8, [4, 5, 6, 7, 8]),
                                          (9, [5, 6, 7, 8, 9]),
                                          (10, [6, 7, 8, 9])))

    @tt.gen_test
    def test_window_size_too_big(self):
        values = [i for i in range(10)]
        chan = channels.IterChannel(iter(values)).windowby(rolling(15)).filter(exact_group_size(15))
        yield self.verify_channel_values(chan, ())

    @tt.gen_test
    def test_empty_source(self):
        values = []
        chan = channels.IterChannel(iter(values)).windowby(rolling(5))
        yield self.verify_channel_values(chan, ())
