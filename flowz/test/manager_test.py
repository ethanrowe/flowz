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


class ChannelBuildHelperCases(object):
    """
    Test cases for use of channel property/method decorator helpers

    Doesn't test anything by itself; in order to use these tests,
    extend this class and implement:
    - build_channel_accessors, such that it defines "get_CHANNEL" methods
      per name in HELPER_NAMES that when called returns the corresponding
      channel, accessing it through the helper-provided interface (which
      will either be a method of a property)
    - the channel builders themselves for each channel named in HELPER_NAMES,
      using property-based helper or method-oriented helper according to
      your purpose.

    The tests will use these get_foo() methods to access the channel in the
    appropriate manner.
    """

    HELPER_NAMES = ('base_a', 'base_b', 'child_a', 'child_b', 'final')

    def build_channel_accessors(self):
        def failer(onname):
            def fail():
                raise NotImplementedError('No accessors for %s', onname)
            return fail

        for name in self.HELPER_NAMES:
            setattr(self, 'get_%s' % name, failer(name))

    
    def setup(self):
        self.channel_manager = management.ChannelManager()
        self.builders = self.assemble_builders()
        self.build_channel_accessors()


    def assemble_builders(self):
        return dict(
                (name, mock.Mock(name='Builder<%s>' % name,
                    return_value=mock_channel('Channel<%s>' %name)))
                for name in self.HELPER_NAMES)

    def build(self, name, *args):
        return self.builders[name](*args)

    def chan(self, name):
        return self.builders[name].return_value

    def verify_channel(self, name, callno):
        expect = self.builders[name].return_value
        if callno > 0:
            expect = getattr(expect.tee, 'call%d' % (callno-1))
    
    def test_basic_channels(self):
        # First access to base_a
        tools.assert_equal(self.chan('base_a'), self.get_base_a())
        # Second access to base_a should be a tee
        tools.assert_equal(self.chan('base_a').tee.call0, self.get_base_a())
        # First access to base_b
        tools.assert_equal(self.chan('base_b'), self.get_base_b())
        # Second access to base b should be a tee
        tools.assert_equal(self.chan('base_b').tee.call0, self.get_base_b())
        # Each builder should be called only once.
        self.builders['base_a'].assert_called_once_with()
        self.builders['base_b'].assert_called_once_with()


    def test_composed_channels(self):
        # First access to each of child_a, child_b
        tools.assert_equal(self.chan('child_a'), self.get_child_a())
        tools.assert_equal(self.chan('child_b'), self.get_child_b())

        # First access to each of base_a, base_b in getting child.
        self.builders['child_a'].assert_called_once_with(self.chan('base_a'))
        self.builders['child_b'].assert_called_once_with(self.chan('base_b'))

        # First access to final
        tools.assert_equal(self.chan('final'), self.get_final())
        # Second access to final
        tools.assert_equal(self.chan('final').tee.call0, self.get_final())

        # Second access to each of child_a, child_b in building final
        self.builders['final'].assert_called_once_with(
                self.chan('child_a').tee.call0,
                self.chan('child_b').tee.call0)


class TestChannelProperty(ChannelBuildHelperCases):
    """
    ChannelBuildHelperCases test using channelproperty to build channels.

    The get_foo() accessors simply retrieve the corresponding property.
    """
    def build_channel_accessors(self):
        def accessor(onname):
            return lambda: getattr(self, onname)
        for name in self.HELPER_NAMES:
            setattr(self, 'get_%s' % name, accessor(name))

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




MANAGER_NAME = str(mock.Mock(name='AChannelManager'))
prop = management.channelproperty(MANAGER_NAME)
meth = management.channelmethod(MANAGER_NAME)

class TestChannelPropertyAlternateName(TestChannelProperty):
    def setup(self):
        setattr(self, MANAGER_NAME, management.ChannelManager())
        self.builders = self.assemble_builders()
        self.build_channel_accessors()

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


class TestChannelMethod(ChannelBuildHelperCases):
    """
    ChannelBuildHelperCases test using channelmethod to build channels.

    The get_foo() accessors are just lambdas on top of the underlying methods.
    """
    def build_channel_accessors(self):
        for name in self.HELPER_NAMES:
            setattr(self, 'get_%s' % name, getattr(self, name))

    @management.channelmethod
    def base_a(self):
        return self.build('base_a')

    @management.channelmethod
    def base_b(self):
        return self.build('base_b')

    @management.channelmethod
    def child_a(self):
        return self.build('child_a', self.base_a())

    @management.channelmethod
    def child_b(self):
        return self.build('child_b', self.base_b())

    @management.channelmethod
    def final(self):
        return self.build('final', self.child_a(), self.child_b())


class TestChannelMethodAlternateName(TestChannelMethod):
    def setup(self):
        setattr(self, MANAGER_NAME, management.ChannelManager())
        self.builders = self.assemble_builders()
        self.build_channel_accessors()

    @meth
    def base_a(self):
        return self.build('base_a')

    @meth
    def base_b(self):
        return self.build('base_b')

    @meth
    def child_a(self):
        return self.build('child_a', self.base_a())

    @meth
    def child_b(self):
        return self.build('child_b', self.base_b())

    @meth
    def final(self):
        return self.build('final', self.child_a(), self.child_b())


class IgnoreMe(object):
    @management.channelmethod
    def method_to_ignore(self):
        pass


class OneOfBoth(object):
    def __init__(self):
        super(OneOfBoth, self).__init__()
        # This tests to make sure that have an attribute that is a channelmethod passed
        # from *another* object is not seen as a channelmethod for this object
        self.should_ignore = IgnoreMe().method_to_ignore

    @management.channelproperty
    def prop(self):
        pass

    @management.channelmethod
    def method(self):
        pass


def test_get_channelmethods():
    # This test not integrated into classes above because of monkey business creating get_* methods
    expected = ChannelBuildHelperCases.HELPER_NAMES
    tools.assert_equals(management.get_channelmethods(TestChannelMethod()), expected)
    tools.assert_equals(management.get_channelmethods(TestChannelMethodAlternateName()), expected)
    tools.assert_equals(management.get_channelmethods(TestChannelProperty()), tuple())
    tools.assert_equals(management.get_channelmethods(TestChannelPropertyAlternateName()), tuple())

    tools.assert_equals(management.get_channelmethods(OneOfBoth()), ('method',))


def test_get_channelproperties():
    expected = ChannelBuildHelperCases.HELPER_NAMES
    tools.assert_equals(management.get_channelproperties(TestChannelMethod()), tuple())
    tools.assert_equals(management.get_channelproperties(TestChannelMethodAlternateName()), tuple())
    tools.assert_equals(management.get_channelproperties(TestChannelProperty()), expected)
    tools.assert_equals(management.get_channelproperties(TestChannelPropertyAlternateName()), expected)

    tools.assert_equals(management.get_channelproperties(OneOfBoth()), ('prop',))
