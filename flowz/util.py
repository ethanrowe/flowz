from __future__ import absolute_import
from __future__ import print_function


class Minimum(object):
    """
    Less than all other objects other than itself.
    """
    def __gt__(self, other):
        return False
    
    def __lt__(self, other):
        return self is not other

    def __eq__(self, other):
        return self is other
    
    def __ne__(self, other):
        return self is not other
    
    def __ge__(self, other):
        return self is other
    
    def __le__(self, other):
        return True


class Maximum(object):
    """
    Greater than all other objects other than itself.
    """
    def __gt__(self, other):
        return self is not other

    def __lt__(self, other):
        return False
    
    def __eq__(self, other):
        return self is other
    
    def __ne__(self, other):
        return self is not other
    
    def __ge__(self, other):
        return True
    
    def __le__(self, other):
        return self is other

MINIMUM = Minimum()
MAXIMUM = Maximum()

NO_VALUE = object()


class LastResult(object):
    """
    Wrap a function into an iterative consumer of its own output.

    Given callables `func` and `first`, the `LastResult` object will act as a callable.

    On first call, it will pass call parameters through to `first`, returning the
    result.

    Subsequent calls will pass call parameters through to `func`, but with the result
    of the previous invocation included as the final positional parameter.

    So, for instance:

        f = LastResult(lambda a, l: a + l, lambda a: a)

        # returns 10 (lambda a: a)
        f(10)

        # returns 11 (lambda a, l: a + l, with a 1 and l 10)
        f(1)

        # returns 13 (a is 2, l is 11 from previous call)
        f(2)

    The `first` callable is optional; when not given, `func` is called for all
    invocations, but for the very first call, the constant `flowz.util.NO_VALUE` is
    used in place of the nonexistent previous result.

    The following would give exactly the same behavior as before:

        f = LastResult(lambda a, l: a if l is NO_VALUE else a + l)

    Enjoy your stateful callable.  May it serve you well.
    """

    def __init__(self, func, first=None):
        first = self.determine_first_call(func, first)
        self.next_call = self.build_first_call(first)
        self.trailing_call = self.build_trailing_call(func)
    
    @classmethod
    def determine_first_call(cls, func, firstfunc):
        if firstfunc is None:
            firstfunc = lambda *a, **kw: func(*(a + (NO_VALUE,)), **kw)
        return firstfunc

    def build_first_call(self, firstfunc):
        def first_call(*a, **kw):
            r = firstfunc(*a, **kw)
            self.next_call = self.trailing_call
            return r
        return first_call

    def build_trailing_call(self, func):
        def trailing_call(*a, **kw):
            return func(*(a + (self.last_result,)), **kw)
        return trailing_call

    def __call__(self, *args, **kw):
        r = self.next_call(*args, **kw)
        self.last_result = r
        return r


def incremental_assembly(source, dest, assembler):
    passthru = lambda curr, last: curr

    # Source chan becomes pair where value is assembler, item pair.
    source = source.map(lambda i: (i[0], (i, assembler)))

    # Dest chan becomes pair where value is passthru, item pair.
    dest = dest.map(lambda i: (i[0], (i, passthru)))

    # Normal merge.
    out = merge_keyed_channels(source, dest)

    return out.map(LastResult(lambda (k, (v, fn)), last: fn(v, last)))


def channel_join(a, b):
    out = a.cogroup(b)
    return out.filter(lambda (x, y): (x is not None and y is not None and x[0] == y[0]))


def _merge_picker(pairs):
    best = None
    pairs = (pair for pair in pairs if pair is not None)
    for pair in pairs:
        if best is None:
            best = pair
        elif pair[0] >= best[0]:
            best = pair
    return best


def merge_keyed_channels(first_chan, other_chan, *other_chans):
    """
    Reduce two or more channels of (key, value) pairs to single channel.

    Given two or more channels where each channel's items are structured as
    (key, value) pairs, and all channels use equivalent keys and an equivalent
    sort order, cogroups the channels and for each cogrouping combination
    passes along the "best" k/v pair, where "best" is the rightmost pair
    (according to channel position in the inputs) with the highest key.
    """
    return first_chan.cogroup(other_chan, *other_chans).map(_merge_picker)


def channel_puller(chan, mode='ensure'):
    # Maybe a temporal item (key/value pair)
    chan = chan.map(lambda x: getattr(x, 'value', x))
    # Maybe an artifact.
    chan = chan.map(lambda y: getattr(y, mode)() if hasattr(y, mode) else y)
    # Wait for em.
    return chan.each_ready().map(print)
