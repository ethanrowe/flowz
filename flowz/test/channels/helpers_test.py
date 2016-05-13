import mock
from nose import tools

from flowz import channels as chn

class TestChannelHelpers(object):
    def setup(self):
        self.channel = self.get_channel()


    def get_channel(self):
        c = chn.Channel(mock.Mock())
        return c


    def verify_delegation(self, result, mock_cls, *args):
        # The result is of the proper class
        tools.assert_equal(mock_cls.return_value, result)
        # The class was invoked with expected args.
        mock_cls.assert_called_once_with(*args)


    def test_tee_method(self):
        with mock.patch.object(chn, 'TeeChannel') as tc:
            self.verify_delegation(self.channel.tee(),
                    tc,
                    self.channel)


    def test_map_method(self):
        func = mock.Mock(name='MapperFunction')
        with mock.patch.object(chn, 'MapChannel') as mc:
            self.verify_delegation(self.channel.map(func),
                    mc,
                    self.channel, func)


    def test_flat_map_method(self):
        func = mock.Mock(name='FlatMapperFunction')
        with mock.patch.object(chn, 'FlatMapChannel') as fmc:
            self.verify_delegation(self.channel.flat_map(func),
                    fmc,
                    self.channel, func)


    def test_each_ready_method(self):
        with mock.patch.object(chn, 'FutureChannel') as fc:
            self.verify_delegation(self.channel.each_ready(),
                    fc,
                    self.channel)


    def test_as_ready_method(self):
        with mock.patch.object(chn, 'ReadyFutureChannel') as rfc:
            self.verify_delegation(self.channel.as_ready(),
                    rfc,
                    self.channel)


    def test_zip_method(self):
        chans = [mock.Mock(name='Channel%d' % i) for i in range(3)]
        with mock.patch.object(chn, 'ZipChannel') as zc:
            self.verify_delegation(self.channel.zip(*chans),
                    zc,
                    [self.channel] + chans)


    def test_cogroup_method(self):
        chans = [mock.Mock(name='Channel%d' % i) for i in range(2)]
        with mock.patch.object(chn, 'CoGroupChannel') as cgc:
            self.verify_delegation(self.channel.cogroup(*chans),
                    cgc,
                    [self.channel] + chans)


    def test_filter_method(self):
        func = mock.Mock(name='PredicateFunction')
        with mock.patch.object(chn, 'FilterChannel') as fc:
            self.verify_delegation(self.channel.filter(func),
                    fc,
                    self.channel, func)


    def test_index_helper(self):
        with mock.patch.object(chn, 'MapChannel') as mc:
            idx = mock.Mock(name='Index')
            r = self.channel[idx]
            # We get a mapchannel back
            tools.assert_equal(mc.return_value, r)
            # It was created with two args, the first of which
            # is the channel itself.
            tools.assert_equal(2, len(mc.call_args[0]))
            tools.assert_equal(self.channel, mc.call_args[0][0])
            # The second argument is a mapper function that looks up
            # the specified index from its argument.
            func  = mc.call_args[0][1]
            val = mock.Mock(name='Value')
            tools.assert_equal(val, func({idx: val}))



class TestTeeChannelHelpers(TestChannelHelpers):
    CLASS = chn.TeeChannel

    def get_channel(self):
        c = mock.Mock()
        c.__future__ = mock.Mock(name='MockFuture')
        return self.CLASS(c)


class TestReadChannelHelpers(TestChannelHelpers):
    CLASS = chn.ReadChannel

    def get_channel(self):
        return self.CLASS(mock.Mock(name='MockChannel'))


class TestFutureChannelHelpers(TestReadChannelHelpers):
    CLASS = chn.FutureChannel


class TestReadyFutureChannelHelpers(TestReadChannelHelpers):
    CLASS = chn.ReadyFutureChannel


class TestProducerChannelHelpers(TestReadChannelHelpers):
    CLASS = chn.ProducerChannel


class TestIterChannelHelpers(TestReadChannelHelpers):
    CLASS = chn.IterChannel


class TestZipChannelHelpers(TestChannelHelpers):
    CLASS = chn.ZipChannel

    def get_channel(self):
        chans = [mock.Mock(name='MockChannel%d' % i) for i in range(5)]
        return self.CLASS(chans)


class TestCoGroupChannelHelpers(TestZipChannelHelpers):
    CLASS = chn.CoGroupChannel


class TestMapChannelHelpers(TestChannelHelpers):
    CLASS = chn.MapChannel

    def get_channel(self):
        return self.CLASS(
                mock.Mock(name='MockChannel'), mock.Mock(name='MockFunction'))


class TestFlatMapChannelHelpers(TestChannelHelpers):
    CLASS = chn.FlatMapChannel


class TestFilterChannelHelpers(TestMapChannelHelpers):
    CLASS = chn.FilterChannel

