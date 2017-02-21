from __future__ import absolute_import
from __future__ import print_function

from nose import tools
from tornado import testing as tt

from flowz.channels import IterChannel
from flowz.util import channel_inner_join, incremental_assembly, merge_keyed_channels, NO_VALUE

from ..channels.util import drain


class AssemblyTest(tt.AsyncTestCase):
    """
    Test various channel assembling routines
    """

    def get_two_chans(self):
        chan1 = IterChannel([(1, 1), (2, 2), (3, 3), (4, 4)])
        chan2 = IterChannel([        (2, 1),         (4, 2), (6, 3), (8, 4)])
        return chan1, chan2

    @tt.gen_test
    def test_merge_keyed_channels(self):
        chan1, chan2 = self.get_two_chans()
        chan3 = merge_keyed_channels(chan1, chan2)
        values = yield drain(chan3)
        tools.assert_equal(values, [(1, 1), (2, 1), (3, 3), (4, 2), (6, 3), (8, 4)])

    @tt.gen_test
    def test_channel_inner_join(self):
        chan1, chan2 = self.get_two_chans()
        joined = channel_inner_join(chan1, chan2)
        values = yield drain(joined)
        tools.assert_equal(values, [((2, 2), (2, 1)), ((4, 4), (4, 2))])
        chan1, chan2 = self.get_two_chans()
        chan3, chan4 = self.get_two_chans()
        chan5, chan6 = self.get_two_chans()
        joined = channel_inner_join(chan1, chan2, chan3, chan4, chan5, chan6)
        values = yield drain(joined)
        tools.assert_equal(values, [((2, 2), (2, 1), (2, 2), (2, 1), (2, 2), (2, 1)),
                                    ((4, 4), (4, 2), (4, 4), (4, 2), (4, 4), (4, 2))])

    @tt.gen_test
    def test_incremental_assembly(self):
        def assemble(curr, last):
            if last == NO_VALUE:
                return curr[0], (curr[1],)
            else:
                return curr[0], last[1] + (curr[1],)

        source, _ = self.get_two_chans()
        dest = IterChannel([])
        chan3 = incremental_assembly(source, dest, assemble)
        values = yield drain(chan3)
        tools.assert_equal(values, [(1, (1,)), (2, (1, 2)), (3, (1, 2, 3)), (4, (1, 2, 3, 4))])
