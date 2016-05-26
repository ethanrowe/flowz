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


def test_channel_manager_builders():
    chan_a, chan_b = mock_channel(), mock_channel()
    key_a, key_b = mock.Mock(), mock.Mock()
    # Shows that generative syntax works by passing self along.
    mgr = management.ChannelManager() \
            .add_builder(key_a, lambda: chan_a) \
            .add_builder(key_b, lambda: chan_b)

    # Build/return on first access
    tools.assert_equal(chan_a, mgr[key_a])
    # Tee on subsequent access
    tools.assert_equal(chan_a.tee.call0, mgr[key_a])
    tools.assert_equal(chan_a.tee.call1, mgr[key_a])

    tools.assert_equal(chan_b, mgr[key_b])
    tools.assert_equal(chan_b.tee.call0, mgr[key_b])
    tools.assert_equal(chan_b.tee.call1, mgr[key_b])


def test_channel_manager_accessors():
    access_a, access_b = mock.Mock(), mock.Mock()
    key_a, key_b = mock.Mock(), mock.Mock()

    # Shows chaining syntax
    mgr = management.ChannelManager() \
            .add_accessor(key_a, access_a) \
            .add_accessor(key_b, access_b)

    # Get access delegates to the accessor's get()
    tools.assert_equal(access_a.get.return_value, mgr[key_a])
    tools.assert_equal(access_b.get.return_value, mgr[key_b])


def test_channel_manager_channels():
    chan_a, chan_b = mock_channel(), mock_channel()
    key_a, key_b = mock.Mock(), mock.Mock()

    mgr = management.ChannelManager() \
            .add_channel(key_a, chan_a) \
            .add_channel(key_b, chan_b)

    tools.assert_equal(chan_a, mgr[key_a])
    tools.assert_equal(chan_b, mgr[key_b])
    tools.assert_equal(chan_a.tee.call0, mgr[key_a])
    tools.assert_equal(chan_b.tee.call0, mgr[key_b])
    tools.assert_equal(chan_a.tee.call1, mgr[key_a])
    tools.assert_equal(chan_b.tee.call1, mgr[key_b])


