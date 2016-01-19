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


def funcs_targets(num=1):
    f = list(mock_func(num))
    return f, [targets.CallTarget(func) for func in f]


def funcs_targets_dicts(keys):
    f, t = funcs_targets(len(keys))
    return dict(zip(keys, f)), dict(zip(keys, t))


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

class TestFuncTarget(NoPositionals, NoKwargs, tt.AsyncTestCase):
    @classmethod
    def args(cls):
        f, t = funcs_targets(cls.POSITIONALS)
        kw_f, kw_t = funcs_targets_dicts(cls.KEYS)
        return f, t, kw_f, kw_t

    @classmethod
    def prepare_target(cls, args, kwargs):
        f, = mock_func(1)
        t = targets.FuncTarget(f, *args, **kwargs)
        return f, t

    @tt.gen_test
    def test_func_target(self):
        pos_f, pos_t, kw_f, kw_t = self.args()
        func, target = self.prepare_target(pos_t, kw_t)

        # We expect the target's func to be called with the results
        # of each arg's future.
        x_args = [f.return_value for f in pos_f]
        x_kwargs = dict((k, f.return_value) for k, f in kw_f.items())

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


