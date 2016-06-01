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
