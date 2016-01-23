import itertools

import mock
from nose import tools
from tornado import gen
from tornado import testing as tt

from flo import targets

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

class TestFuncTarget(AllFutures, NoPositionals, NoKwargs, tt.AsyncTestCase):
    @classmethod
    def args(cls):
        arg = cls.targets_and_values(cls.POSITIONALS)
        kw = cls.targets_and_values_dict(cls.KEYS)
        return arg, kw

    @classmethod
    def prepare_target(cls, args, kwargs):
        f, = mock_func(1)
        t = targets.FuncTarget(f, *args, **kwargs)
        return f, t

    @tt.gen_test
    def test_func_target(self):
        pos, kw = self.args()
        func, target = self.prepare_target(pos, kw)

        # We expect the target's func to be called with the results
        # of each pertinent arg's future.
        x_args = self.expect_args(pos)
        x_kwargs = self.expect_kwargs(kw, self.KEYS)

        for _ in range(2):
            r = yield target.future()
            tools.assert_equal(func.return_value, r)

        func.assert_called_once_with(*x_args, **x_kwargs)


class TestFuncTwoPos(TwoPositionals, TestFuncTarget):
    pass

class TestFuncTwoPosOdds(OddFutures, TestFuncTwoPos):
    pass

class TestFuncTwoPosEvens(EvenFutures, TestFuncTwoPos):
    pass

class TestFunTwoPosNone(NoFutures, TestFuncTarget):
    pass

class TestFuncThreePos(ThreePositionals, TestFuncTarget):
    pass

class TestFuncThreePosOdds(OddFutures, TestFuncThreePos):
    pass

class TestFuncThreePosEvens(EvenFutures, TestFuncThreePos):
    pass

class TestFuncThreePosNone(NoFutures, TestFuncThreePos):
    pass

class TestFuncOneKwarg(OneKwarg, TestFuncTarget):
    pass

class TestFuncOneKwargNone(NoFutures, TestFuncOneKwarg):
    pass

class TestFuncTwoKwargs(TwoKwargs, TestFuncTarget):
    pass

class TestFuncTwoKwargsOdds(OddFutures, TestFuncTwoKwargs):
    pass

class TestFuncTwoKwargsEvents(EvenFutures, TestFuncTwoKwargs):
    pass

class TestFuncTwoKwargs(NoFutures, TestFuncTwoKwargs):
    pass

class TestFuncThreeKwargs(ThreeKwargs, TestFuncTarget):
    pass

class TestFuncThreeKwargsOdds(OddFutures, TestFuncThreeKwargs):
    pass

class TestFuncThreeKwargsEvens(EvenFutures, TestFuncThreeKwargs):
    pass

class TestFuncOnePosOneKwarg(OnePositional, OneKwarg, TestFuncTarget):
    pass

class TestFuncOnePosOneKwargNone(NoFutures, TestFuncOnePosOneKwarg):
    pass

class TestFuncOnePosThreeKwargs(OnePositional, ThreeKwargs, TestFuncTarget):
    pass

class TestFuncOnePosThreeKwargsOdds(OddFutures, TestFuncOnePosThreeKwargs):
    pass

class TestFuncOnePosThreeKwargsEvens(EvenFutures, TestFuncOnePosThreeKwargs):
    pass

class TestFuncOnePosThreeKawrgsNone(NoFutures, TestFuncOnePosThreeKwargs):
    pass

class TestFuncThreePosOneKwarg(ThreePositionals, OneKwarg, TestFuncTarget):
    pass

class TestFuncThreePosOneKwargOdds(OddFutures, TestFuncThreePosOneKwarg):
    pass

class TestFuncThreePosOneKwargEvens(EvenFutures, TestFuncThreePosOneKwarg):
    pass

class TestFuncThreePosOneKwargNone(NoFutures, TestFuncThreePosOneKwarg):
    pass

class TestFuncThreePosThreeKwargs(ThreePositionals, ThreeKwargs, TestFuncTarget):
    pass

class TestFuncThreePosThreeKwargsOdds(OddFutures, TestFuncThreePosThreeKwargs):
    pass

class TestFuncThreePosThreeKwargsEvens(EvenFutures, TestFuncThreePosThreeKwargs):
    pass

class TestFuncThreePosThreeKwargsNone(NoFutures, TestFuncThreePosThreeKwargs):
    pass

