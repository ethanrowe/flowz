import itertools

import mock
from nose import tools
from tornado import gen
from tornado import testing as tt

from flowz import targets

COUNTER = itertools.count()

def mock_func(num=1, template='MockFunction%d'):
    for x in range(num):
        yield mock.Mock(name=template % next(COUNTER), spec={})

def futurize(m, returnval=None):
    @gen.coroutine
    def future():
        if returnval is None:
            return m.future.return_value
        return returnval
    m.future = mock.Mock(name='future%d' % next(COUNTER))
    m.future.side_effect = future
    return m


def targets_and_values(num=1, futures=()):
    f = list(mock_func(num, 'Param%d'))
    for i in futures:
        futurize(f[i])
    return f


def targets_and_values_dict(keys, futures=()):
    f = dict(zip(keys, targets_and_values(len(keys))))
    for k in futures:
        futurize(f[k])
    return f


def func_target(args, kwargs):
    f, = mock_func(1)
    return f, targets.FuncTarget(f, *args, **kwargs)


class TestBasicTargets(tt.AsyncTestCase):
    @tt.gen_test
    def test_future_value_and_caching(self):
        class RealTarget(targets.Target):
            call = mock.Mock(name='Call')

            @gen.coroutine
            def start(self):
                raise gen.Return(self.call())

        t = RealTarget()

        r = yield t.future()

        tools.assert_equal(RealTarget.call.return_value, r)
        
        r = yield t.future()

        RealTarget.call.assert_called_once_with()


    @tt.gen_test
    def test_simple_func_caller(self):
        func, = mock_func()
        t = targets.CallTarget(func,)

        for _ in range(2):
            r = yield t.future()
            tools.assert_equal(func.return_value, r)

        func.assert_called_once_with()


class NoPositionals(object):
    POSITIONALS = 0

class NoKwargs(object):
    KEYS = ()


class WithFutures(object):
    @classmethod
    def targets_and_values(cls, t):
        return targets_and_values(t, cls.futures_indices(t))

    @classmethod
    def targets_and_values_dict(cls, keys):
        return targets_and_values_dict(keys,
                [keys[i] for i in cls.futures_indices(len(keys))])

    @classmethod
    def expect_args(cls, args):
        idx = cls.futures_indices(len(args))
        expect = list(args)
        for i in idx:
            expect[i] = expect[i].future.return_value
        return expect
    
    @classmethod
    def expect_kwargs(cls, kw, keys):
        idx = cls.futures_indices(len(keys))
        expect = dict(kw)
        for i in idx:
            expect[keys[i]] = expect[keys[i]].future.return_value
        return expect


class NoFutures(WithFutures):
    @classmethod
    def futures_indices(cls, t):
        return []


class AllFutures(WithFutures):
    @classmethod
    def futures_indices(cls, t):
        return list(range(t))


class EvenFutures(WithFutures):
    @classmethod
    def futures_indices(cls, t):
        return list(range(0, t, 2))


class OddFutures(WithFutures):
    @classmethod
    def futures_indices(cls, t):
        return list(range(1, t, 2))


class OnePositional(object):
    POSITIONALS = 1

class TwoPositionals(object):
    POSITIONALS = 2

class ThreePositionals(object):
    POSITIONALS = 3

class OneKwarg(object):
    KEYS = ('a',)

class TwoKwargs(object):
    KEYS = ('b', 'c')

class ThreeKwargs(object):
    KEYS = ('d', 'e', 'f')

class TestFutureParamTarget(AllFutures, NoPositionals, NoKwargs, tt.AsyncTestCase):
    @classmethod
    def args(cls):
        arg = cls.targets_and_values(cls.POSITIONALS)
        kw = cls.targets_and_values_dict(cls.KEYS)
        return arg, kw

    @classmethod
    def prepare_func_target(cls, args, kwargs):
        f, = mock_func(1)
        t = targets.FuncTarget(f, *args, **kwargs)
        return f, t


    @classmethod
    def expected_params(cls, pos, kw):
        return cls.expect_args(pos), cls.expect_kwargs(kw, cls.KEYS)


    @tt.gen_test
    def test_func_target(self):
        pos, kw = self.args()
        func, target = self.prepare_func_target(pos, kw)

        # We expect the target's func to be called with the results
        # of each pertinent arg's future.
        x_args, x_kwargs = self.expected_params(pos, kw)

        for _ in range(2):
            r = yield target.future()
            tools.assert_equal(func.return_value, r)

        func.assert_called_once_with(*x_args, **x_kwargs)


    @tt.gen_test
    def test_future_target(self):
        pos, kw = self.args()

        f, = mock_func(1)
        futurize(f.return_value)
        t = targets.FutureTarget(f, *pos, **kw)

        x_args, x_kwargs = self.expected_params(pos, kw)

        for _ in range(2):
            r = yield t.future()
            tools.assert_equal(f.return_value.future.return_value, r)

        f.assert_called_once_with(*x_args, **x_kwargs)
        f.return_value.future.assert_called_once_with()


class TestFutureParamTwoPos(TwoPositionals, TestFutureParamTarget):
    pass

class TestFutureParamTwoPosOdds(OddFutures, TestFutureParamTwoPos):
    pass

class TestFutureParamTwoPosEvens(EvenFutures, TestFutureParamTwoPos):
    pass

class TestFunTwoPosNone(NoFutures, TestFutureParamTarget):
    pass

class TestFutureParamThreePos(ThreePositionals, TestFutureParamTarget):
    pass

class TestFutureParamThreePosOdds(OddFutures, TestFutureParamThreePos):
    pass

class TestFutureParamThreePosEvens(EvenFutures, TestFutureParamThreePos):
    pass

class TestFutureParamThreePosNone(NoFutures, TestFutureParamThreePos):
    pass

class TestFutureParamOneKwarg(OneKwarg, TestFutureParamTarget):
    pass

class TestFutureParamOneKwargNone(NoFutures, TestFutureParamOneKwarg):
    pass

class TestFutureParamTwoKwargs(TwoKwargs, TestFutureParamTarget):
    pass

class TestFutureParamTwoKwargsOdds(OddFutures, TestFutureParamTwoKwargs):
    pass

class TestFutureParamTwoKwargsEvents(EvenFutures, TestFutureParamTwoKwargs):
    pass

class TestFutureParamTwoKwargs(NoFutures, TestFutureParamTwoKwargs):
    pass

class TestFutureParamThreeKwargs(ThreeKwargs, TestFutureParamTarget):
    pass

class TestFutureParamThreeKwargsOdds(OddFutures, TestFutureParamThreeKwargs):
    pass

class TestFutureParamThreeKwargsEvens(EvenFutures, TestFutureParamThreeKwargs):
    pass

class TestFutureParamOnePosOneKwarg(OnePositional, OneKwarg, TestFutureParamTarget):
    pass

class TestFutureParamOnePosOneKwargNone(NoFutures, TestFutureParamOnePosOneKwarg):
    pass

class TestFutureParamOnePosThreeKwargs(OnePositional, ThreeKwargs, TestFutureParamTarget):
    pass

class TestFutureParamOnePosThreeKwargsOdds(OddFutures, TestFutureParamOnePosThreeKwargs):
    pass

class TestFutureParamOnePosThreeKwargsEvens(EvenFutures, TestFutureParamOnePosThreeKwargs):
    pass

class TestFutureParamOnePosThreeKawrgsNone(NoFutures, TestFutureParamOnePosThreeKwargs):
    pass

class TestFutureParamThreePosOneKwarg(ThreePositionals, OneKwarg, TestFutureParamTarget):
    pass

class TestFutureParamThreePosOneKwargOdds(OddFutures, TestFutureParamThreePosOneKwarg):
    pass

class TestFutureParamThreePosOneKwargEvens(EvenFutures, TestFutureParamThreePosOneKwarg):
    pass

class TestFutureParamThreePosOneKwargNone(NoFutures, TestFutureParamThreePosOneKwarg):
    pass

class TestFutureParamThreePosThreeKwargs(ThreePositionals, ThreeKwargs, TestFutureParamTarget):
    pass

class TestFutureParamThreePosThreeKwargsOdds(OddFutures, TestFutureParamThreePosThreeKwargs):
    pass

class TestFutureParamThreePosThreeKwargsEvens(EvenFutures, TestFutureParamThreePosThreeKwargs):
    pass

class TestFutureParamThreePosThreeKwargsNone(NoFutures, TestFutureParamThreePosThreeKwargs):
    pass

