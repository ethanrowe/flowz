from __future__ import absolute_import

import itertools
import mock
from nose import tools

from flowz.channels import management

def mock_channel():
    """
    A mock that supports tee()

    Each successive call to the mock's tee() will give m.tee.callCALL_COUNT where
    CALL_COUNT is the string version of the current count of the tee call.  That
    count starts at 0 and increments by 1 per tee call.
    """
    m = mock.Mock()
    m.tee_count = iter(itertools.count(0))
    m.tee.side_effect = lambda: getattr(m.tee, 'call%d' % next(m.tee_count))
    return m


def test_channel_accessor():
    # Our channel
    chan = mock_channel()
    # Accessor gets a "builder" function that returns the channel.
    accessor = management.ChannelAccessor(lambda: chan)

    # First call builds and returns the original channel.
    tools.assert_equal(chan, accessor.get())

    # Second call returns the first tee
    tools.assert_equal(chan.tee.call0, accessor.get())

    # Third call returns the second tee
    tools.assert_equal(chan.tee.call1, accessor.get())

    # Fourth call returns the third tee
    tools.assert_equal(chan.tee.call2, accessor.get())


