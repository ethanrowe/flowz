import mock
from nose import tools
from tornado import gen
from tornado import testing as tt

from flowz import channels

@gen.coroutine
def drain(channel, per_read=lambda x: x):
    received = []
    try:
        while True:
            value = yield channel.next()
            received.append(value)
            per_read(value)

    except channels.ChannelDone:
        raise gen.Return(received)

def fixtures(vals):
    vals = list(vals)
    reads = [None]
    lagged = reads + vals[:-1]
    pairs = list(zip(vals, lagged))
    return vals, pairs, reads


class ReadBoundChannelsTest(tt.AsyncTestCase):
    @tt.gen_test
    def test_map_channel(self):
        vals, expect, reads = fixtures(range(5))

        chan = channels.IterChannel(vals)
        chan = channels.MapChannel(chan, lambda i: (i, reads[-1]))

        received = yield drain(chan, lambda (c, l): reads.append(c))

        tools.assert_equal(expect, received)


    @tt.gen_test
    def test_flat_map_channel(self):
        # Grouped stuff.
        vals = [[(outer, inner) for inner in range(3)]
                for outer in range(3)]

        # Flatten the groups for the fixtures.
        _, expect, reads = fixtures((o, i) for grp in vals for o, i in grp)

        chan = channels.IterChannel(vals)
        chan = channels.FlatMapChannel(chan,
                lambda row: ((pair, reads[-1]) for pair in row))

        received = yield drain(chan, lambda (c, l): reads.append(c))

        tools.assert_equal(expect, received)

