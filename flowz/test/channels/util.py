from nose import tools
from tornado import gen

from flowz import channels


@gen.coroutine
def raises_channel_done(channel):
    """
    Tests that a channel has already reached its last item.
    """
    try:
        yield channel.next()
        raise Exception("ChannelDone not raised!")
    except channels.ChannelDone:
        # This just increments the test count,
        tools.assert_equal(True, True)


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
