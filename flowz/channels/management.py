import functools

class AbstractChannelAccessor(object):
    """
    Wraps the access to a :class:`flowz.channels.core.Channel`.

    Given some logic for how to build a particular channel
    (:meth:`AbstractChannelAccessor.get_new`),
    this will manage the lifecycle of the resulting channel:
    - Build it and return it on first access
    - Return a new tee of it on each subsequent access.

    The channel is accessed via the :meth:`AbstractChannelAccesor.get` method.

    Details as to why this matters:

    Garbage collection of items on a channel depends on channels being consumed
    to completion.

    When a `:meth:`flowz.channels.core.Channel.tee` is taken of a channel, both
    the original channel *and* the tee need to be consumed for items to be
    freed for GC.

    The creation of extraneous tees, then, can cause memory leaks.
    """

    channel = None

    def get(self):
        """
        Returns the managed :class:`flowz.channels.core.Channel`.

        Will give the original channel on first access, and a new tee per
        subsequent access.
        """
        if self.channel:
            return self.get_existing()
        return self.get_new()


    def get_existing(self):
        """
        For internal/subclass use, the accessor for subsequent access.
        """
        return self.channel.tee()


    def get_new(self):
        """
        For internal/subclass use, the accessor for building and first access.

        Must be overridden in subclasses to build the
        :class:`flowz.channels.core.Channel`, set it to `self.channel``, and
        return it.
        """
        raise NotImplementedError("get_new() needs to be defined.")



class ChannelAccessor(AbstractChannelAccessor):
    """
    An `AbstractChannelAccessor` implementation with a build function.
    """

    def __init__(self, buildfunc):
        """
        The managed channel will be assembled with :func:`buildfunc`.

        So :func:`buildfunc` must return the intended channel to be managed.
        """
        self.builder = buildfunc


    def get_new(self):
        self.channel = self.builder()
        return self.channel


class ChannelManager(object):
    """
    A keyed collection for managed channels.

    Allows for the assembly of managed channels organized by key (like a dict),
    and access to those channels (via key) will properly manage
    :meth:`flowz.channels.core.Channel.tee` calls as done by
    :class:`AbstractChannelAccessor`
    """

    def __init__(self):
        self.channels = {}

    def add_builder(self, name, buildfunc):
        """
        Adds a :class:`ChannelAccessor` under key ``name``.

        Args:
            name (hashable): the key to use for identifying this managed
                channel.
            buildfunc (callable): the channel builder function to use in
                the underlying :class:`ChannelAccessor`.

        Returns ``self`` (the :class:`ChannelManager`).
        """
        bld = ChannelAccessor(buildfunc)
        return self.add_accessor(name, bld)

    def add_accessor(self, name, accessor):
        """
        Adds the :class:`AbstractChannelAccessor` ``accessor`` under ``name``.

        Args:
            name (hashable): the key to use for identifying this managed
                channel.
            accessor (:class:`AbstractChannelAccessor`): the accessor to use
                for managing access to the underlying channel.

        Returns ``self`` (the :class:`ChannelManager`).
        """
        self.channels[name] = accessor
        return self

    def add_channel(self, name, channel):
        """
        Adds ``channel`` for access management via ``name``.

        Args:
            name (hashable): the key to use for identifying this managed
                channel.
            channel (:class:`flowz.channels.core.Channel`): the channel to
                be access-managed.

        Returns ``self`` (the :class`ChannelManager`).
        """
        self.add_builder(name, lambda: channel)
        return self

    def __getitem__(self, key):
        """
        Access the managed channel associated with ``key``.

        Throws a ``KeyError`` if the ``key`` is unknown.
        """
        try:
            accessor = self.channels[key]
            return accessor.get()
        except KeyError:
            raise KeyError("No such key %s" % repr(key))


def _channelproperty(manager_name, fn):
    @functools.wraps(fn)
    def wrapped(self):
        manager = getattr(self, manager_name)
        name = fn.__name__
        try:
            return manager[name]
        except KeyError:
            manager.add_builder(name, lambda: fn(self))
            return manager[name]
    return property(wrapped)

def channelproperty(fn_or_name):
    """
    Decorates a builder method to act as an access-managed channel.

    Expects to decorate a callable for an object that has a
    :class:`ChannelManager` instance.  The callable should return a
    channel to be managed.

    The result is a property that fronts access to the corresponding
    channel via the :class:`ChannelManager`, meaning that first access
    to the property will cause the build logic to run, returning the
    underlying channel, while each subsequent access to the property will
    return a new :meth:`flowz.channels.core.Channel.tee` result.

    By default, the :class:`ChannelManager` instance is expected to be
    found at the ``channel_manager`` property of the object.  However,
    this can be overridden; call the decorator with an alternate string
    name instead of a function, and you'll get a new decorator function
    bound to that opposite name.

    An example::
        from __future__ import print_function

        from flowz import app
        from flowz.channels import core
        from flowz.channels import management as mgmt

        class StupidExample(object):
            def __init__(self, count):
                self.channel_manager = mgmt.ChannelManager()
                self.count = count


            # We get a channel of numbers from 0 to count.
            @mgmt.channelproperty
            def numbers(self):
                return core.IterChannel(range(self.count))

            # Whereas this is the doubling of those numbers.
            @mgmt.channelproperty
            def doubles(self):
                # self.numbers might be the raw IterChannel, or
                # it might be a tee thereof, depending on what
                # has happened first.
                return self.numbers.map(lambda i: i*2)

            # This pairs them up.  It is guaranteed that there
            # will be at least one tee of self.numbers, because
            # it is accessed twice (once directly, once via doubles)
            @mgmt.channelproperty
            def pairs(self):
                return self.numbers.zip(self.doubles)

        # This will print
        # (0, 0)
        # (1, 2)
        # (2, 4)
        # (3, 6)
        # (4, 8)
        app.Flo([StupidExample(5).pairs.map(print)]).run()

    If you wanted to name your :class:`ChannelManager` something else,
    this would do it::
        class OtherExample(object):
            def __init__(self, count):
                self.mgr = mgmt.ChannelManager()
                self.count = count

            @mgmt.channelproperty('mgr')
            def numbers(self):
                return core.IterChannel(range(self.count))

            @mgmt.channelproperty('mgr')
            def doubles(self):
                return self.numbers.map(lambda i: i*2)

            @mgmt.channelproperty('mgr')
            def pairs(self):
                return self.numbers.zip(self.doubles)

    Args:
        fn_or_name (callable or string): if a callable, the callable to
            be used as the underlying ``buildfunc`` for the managed channel
            associated with this property.  Otherwise, the name of attribute
            at which the :class:`ChannelManager` would be found.

    Returns a new property if `fn_or_name` was a callable, and a new decorator
    function bound to the alternate attribute name otherwise.
    """
    if callable(fn_or_name):
        return _channelproperty('channel_manager', fn_or_name)
    return lambda fn: _channelproperty(fn_or_name, fn)

