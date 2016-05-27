from __future__ import absolute_import

import itertools
import mock
from nose import tools

from flowz.channels import management

def mock_channel(name='mockchannel'):
    """
    A mock that supports tee()

    Each successive call to the mock's tee() will give m.tee.callCALL_COUNT where
    CALL_COUNT is the string version of the current count of the tee call.  That
    count starts at 0 and increments by 1 per tee call.
    """
    m = mock.Mock(name=name)
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


class TestChannelProperty(object):
    BUILDERS = ('base_a', 'base_b', 'child_a', 'child_b', 'final')
    
    def setup(self):
        self.channel_manager = management.ChannelManager()
        self.builders = self.assemble_builders()

    def assemble_builders(self):
        return dict(
                (name, mock.Mock(name='Builder<%s>' % name,
                    return_value=mock_channel('Channel<%s>' %name)))
                for name in self.BUILDERS)

    def build(self, name, *args):
        return self.builders[name](*args)

    def chan(self, name):
        return self.builders[name].return_value

    @management.channelproperty
    def base_a(self):
        return self.build('base_a')

    @management.channelproperty
    def base_b(self):
        return self.build('base_b')

    @management.channelproperty
    def child_a(self):
        return self.build('child_a', self.base_a)

    @management.channelproperty
    def child_b(self):
        return self.build('child_b', self.base_b)

    @management.channelproperty
    def final(self):
        return self.build('final', self.child_a, self.child_b)

    def verify_channel(self, name, callno):
        expect = self.builders[name].return_value
        if callno > 0:
            expect = getattr(expect.tee, 'call%d' % (callno-1))
    
    def test_basic_properties(self):
        # First access to base_a
        tools.assert_equal(self.chan('base_a'), self.base_a)
        # Second access to base_a should be a tee
        tools.assert_equal(self.chan('base_a').tee.call0, self.base_a)
        # First access to base_b
        tools.assert_equal(self.chan('base_b'), self.base_b)
        # Second access to base b should be a tee
        tools.assert_equal(self.chan('base_b').tee.call0, self.base_b)
        # Each builder should be called only once.
        self.builders['base_a'].assert_called_once_with()
        self.builders['base_b'].assert_called_once_with()


    def test_composed_properties(self):
        # First access to each of child_a, child_b
        tools.assert_equal(self.chan('child_a'), self.child_a)
        tools.assert_equal(self.chan('child_b'), self.child_b)

        # First access to each of base_a, base_b in getting child.
        self.builders['child_a'].assert_called_once_with(self.chan('base_a'))
        self.builders['child_b'].assert_called_once_with(self.chan('base_b'))

        # First access to final
        tools.assert_equal(self.chan('final'), self.final)
        # Second access to final
        tools.assert_equal(self.chan('final').tee.call0, self.final)

        # Second access to each of child_a, child_b in building final
        self.builders['final'].assert_called_once_with(
                self.chan('child_a').tee.call0,
                self.chan('child_b').tee.call0)


MANAGER_NAME = str(mock.Mock(name='AChannelManager'))
prop = management.channelproperty(MANAGER_NAME)

class TestChannelPropertyAlternateName(TestChannelProperty):
    def setup(self):
        setattr(self, MANAGER_NAME, management.ChannelManager())
        self.builders = self.assemble_builders()

    @prop
    def base_a(self):
        return self.build('base_a')

    @prop
    def base_b(self):
        return self.build('base_b')

    @prop
    def child_a(self):
        return self.build('child_a', self.base_a)

    @prop
    def child_b(self):
        return self.build('child_b', self.base_b)

    @prop
    def final(self):
        return self.build('final', self.child_a, self.child_b)


