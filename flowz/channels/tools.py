"""
Helper functions to use with channels, to avoid combinatorial explosion of the core classes.
"""

from __future__ import absolute_import
from __future__ import print_function

import sys


class Roller(object):
    def __init__(self, window_size):
        assert window_size > 0
        self.window_size = window_size
        self.count = 0

    def __call__(self, obj):
        for i in range(self.count, self.count+self.window_size):
            yield i
        self.count += 1


def rolling(window_size):
    """
    Return a callable that can be passed to windowby() to put channel objects
    into rolling windows of `window_size`.

    The callable retains internal state, so it should not be used in multiple contexts.

    The return windows will ramp from size 1 up to `window_size`, then back down
    to 1 by the end of the channel.  In order to constrain them to a particular
    size, consider using `pin_group_size()`.
    """
    return Roller(window_size)


def pinned_group_size(lower=0, upper=sys.maxsize):
    """
    Returns a function that will test its argument -- expected to be a (k,g) tuple
    with a key and a collection -- and return True iff the length of the collection
    is between the `lower` and `upper` bounds, inclusive.

    This is intended to used like `chan.filter(pin_group_size(2, 5))`.
    """
    return lambda key_group: lower <= len(key_group[1]) <= upper


def exact_group_size(size):
    """
    Returns a function that will test its argument -- expected to be a (k,g) tuple
    with a key and a collection -- and return True iff the length of the collection
    is exactly `size`

    This is intended to used like `chan.filter(exact_group_size(5))`.
    """
    return pinned_group_size(size, size)
