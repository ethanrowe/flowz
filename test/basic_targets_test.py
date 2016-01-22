import itertools

import mock
from nose import tools
from tornado import gen
from tornado import testing as tt

from flo import targets

COUNTER = itertools.count()

def mock_func(num=1):
    for x in range(num):
        yield mock.Mock(name='MockFunction%d' % next(COUNTER))

def futurize(m):
    @gen.coroutine
    def future():
        return m.future.return_value
    m.future.side_effect = future
    return m


def targets_and_values(num=1, futures=()):
    f = list(mock_func(num))
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
        return list(range(t, 0, 2))


class OddFutures(WithFutures):
    @classmethod
    def futures_indices(cls, t):
        return list(range(t, 1, 2))


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
        # of each arg's future.
        x_args = self.expect_args(pos)
        x_kwargs = self.expect_kwargs(kw, self.KEYS)

        for _ in range(2):
            r = yield target.future()
            tools.assert_equal(func.return_value, r)

        func.assert_called_once_with(*x_args, **x_kwargs)


class TestFuncTwoPos(TwoPositionals, TestFuncTarget):
    pass

class TestFuncThreePos(ThreePositionals, TestFuncTarget):
    pass

class TestFuncOneKwarg(OneKwarg, TestFuncTarget):
    pass

class TestFuncTwoKwargs(TwoKwargs, TestFuncTarget):
    pass

class TestFuncThreeKwargs(ThreeKwargs, TestFuncTarget):
    pass

class TestFuncOnePosOneKwarg(OnePositional, OneKwarg, TestFuncTarget):
    pass

class TestFuncOnePosThreeKwargs(OnePositional, ThreeKwargs, TestFuncTarget):
    pass

class TestFuncThreePosOneKwarg(ThreePositionals, OneKwarg, TestFuncTarget):
    pass

class TestFuncThreePosThreeKwarg(ThreePositionals, ThreeKwargs, TestFuncTarget):
    pass


