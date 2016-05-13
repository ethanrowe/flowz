import contextlib
import itertools
import threading

import mock
from nose import tools
from tornado import gen
from tornado import ioloop
from tornado import testing as tt

from flowz import app
from flowz import channels
from flowz import targets

COUNTER = itertools.count()

class MockTarget(targets.Target):
    def __init__(self, callable_):
        self.callable = callable_

    @gen.coroutine
    def start(self):
        r = self.callable()
        raise gen.Return(r)

class DependentTarget(targets.Target):
    def __init__(self, deps, callable_=None):
        if callable_ is None:
            callable_ = mock.Mock(
                    name='DependentTargetCallable%d' % next(COUNTER))
        self.callable = callable_
        self.deps = list(deps)


    @gen.coroutine
    def start(self):
        if self.deps:
            r = yield [dep.future() for dep in self.deps]
        r = self.callable(r)
        raise gen.Return(r)


class ProducerTarget(targets.Target):
    def __init__(self, flo, targets, callable_=None):
        if callable_ is None:
            callable_ = mock.Mock(
                    name='ProducerTargetCallable%d' % next(COUNTER))
        self.callable = callable_
        self.targets = targets

    @gen.coroutine
    def start(self):
        r = self.callable(r)
        flo.add_targets(self.targets)
        raise gen.Return(r)


def mock_channel(*callables):
    chan = mock.Mock(name='MockChannel%d' % next(COUNTER), spec={})
    chan.tocall = list(callables)
    chan.doneflag = False
    @gen.coroutine
    def _next():
        try:
            yield gen.sleep(0.01)
            raise gen.Return(chan.tocall.pop(0)())
        except IndexError:
            chan.doneflag = True
            raise channels.ChannelDone
    chan.next = mock.Mock()
    chan.done = mock.Mock()
    chan.next.side_effect = _next
    chan.done.side_effect = lambda: chan.doneflag
    return chan


def blow_up(exception):
    raise exception


@contextlib.contextmanager
def enforced_scope(targets):
    yield
    for t in targets:
        t.callable.side_effect = lambda *a, **b: blow_up(
                RuntimeError('Target called out of scope'))


@contextlib.contextmanager
def scoped_targets(num):
    targets = [MockTarget(mock.Mock(name='TargetFunc%d' % i))
            for i in range(num)]
    with enforced_scope(targets):
        yield targets


class Thread(threading.Thread):
    def run(self):
        """
        Quieteth the ecceptiones
        
        This catches all exceptions in the target, such that
        we don't get exception noise on stderr that bypasses
        the test framework (since that exception noise is typically
        from exceptions intentionally thrown).
        """
        try:
            super(Thread, self).run()
        except:
            pass


# The base tornado.testing.AsyncTestCase doesn't fit the
# flo lifecycle well, so this guy manages ioloops itself,
# in separate threads.
class TestFlowApplication(object):
    def do_flow(self, targets, output=None):
        loop = ioloop.IOLoop()
        loop.make_current()
        if callable(targets):
            flo = app.Flo([])
            targets = targets(flo)
            flo.add_targets(targets)
        else:
            flo = app.Flo(targets)
        if output is not None:
            output.append(loop)
            output.append(flo)
        try:
            flo.run()
        except Exception as e:
            if output is not None:
                del output[:]
                output.append(e)
            raise


    def run_flo(self, targets, timeout=0.5):
        passback = []
        thread = Thread(target=self.do_flow,
                args=(targets,), kwargs={'output': passback})
        thread.start()
        thread.join(timeout)
        if thread.isAlive():
            if passback:
                passback[0].stop()
            raise RuntimeError('The flo exceeded the timeout threshold')
        if isinstance(passback[0], Exception):
            raise passback[0]
        return passback


    def test_donuthin(self):
        # No targets.
        # It should immediately return.
        loop, flo = self.run_flo([])

        tools.assert_equal(True, True)


    def test_one_target_no_dep(self):
        func = mock.Mock()
        t = MockTarget(func)

        with scoped_targets(1) as (t,):
            loop, flo = self.run_flo([t])

        t.callable.assert_called_once_with()
        tools.assert_equal(True, t.future().done())
        tools.assert_equal(t.callable.return_value, t.future().result())


    def test_multiple_static_targets(self):
        with scoped_targets(3) as (a, b, c):
            loop, flo = self.run_flo([a, b, c])

        a.callable.assert_called_once_with()
        b.callable.assert_called_once_with()
        c.callable.assert_called_once_with()

        tools.assert_equal(
                [True] * 3,
                [t.future().done() for t in (a, b, c)])

        tools.assert_equal(
                [t.callable.return_value for t in (a, b, c)],
                [t.future().result() for t in (a, b, c)])


    def test_single_channel(self):
        a, b, c = mock.Mock(), mock.Mock(), mock.Mock()
        chan = mock_channel(a, b, c)

        loop, flo = self.run_flo([chan])

        # Next was called four times, it blows up on the fourth.
        tools.assert_equal([((), {})] * 4, chan.next.call_args_list)

        # The underlying guys were invoked.
        a.assert_called_once_with()
        b.assert_called_once_with()
        c.assert_called_once_with()

        tools.assert_true(chan.done())


    def test_two_channels(self):
        a, b, c, d, e, f, g, h = (mock.Mock() for i in range(8))
        chan1 = mock_channel(a, b, c)
        chan2 = mock_channel(d, e, f, g, h)

        loop, flo = self.run_flo([chan1, chan2])

        # Next called four times on first guy.
        tools.assert_equal([((), {})] * 4, chan1.next.call_args_list)

        # But called six times on second guy, since he takes longer to blow up.
        tools.assert_equal([((), {})] * 6, chan2.next.call_args_list)

        tools.assert_true(chan1.done() and chan2.done())


    def test_nested_channels(self):
        chans = []
        def build_targets(flo):
            def spawn(child, result):
                def f():
                    flo.add_targets([child])
                    return result
                return f

            bottom_a = mock_channel(*(mock.Mock() for _ in range(10)))
            bottom_b = mock_channel(*(mock.Mock() for _ in range(7)))

            middle = mock_channel(spawn(bottom_b, 'b'), mock.Mock(),
                    spawn(bottom_a, 'a'))

            top = mock_channel(mock.Mock(), mock.Mock(), spawn(middle, 'm'))

            # Add channel/numitems pairs
            chans.append((top, 3))
            chans.append((middle, 3))
            chans.append((bottom_a, 10))
            chans.append((bottom_b, 7))

            return [top]

        loop, flo = self.run_flo(build_targets)

        for chan, numitems in chans:
            tools.assert_equal([((), {})] * (numitems + 1),
                    chan.next.call_args_list)

            tools.assert_true(chan.done())


    def test_exception_handling(self):
        class CrackWhoresAnonymousException(Exception):
            """
            Ah git mah 3-month coin next week!
            """
            pass

        with scoped_targets(1) as (t,):
            t.callable = lambda: blow_up(
                    CrackWhoresAnonymousException("Crackies!"))
            tools.assert_raises(
                    CrackWhoresAnonymousException,
                    self.run_flo, [t])

        with scoped_targets(1) as (inner,):
            inner.callable = lambda: blow_up(
                    CrackWhoresAnonymousException("Deepa!  Deepa!"))
            t = DependentTarget([inner])
            tools.assert_raises(
                    CrackWhoresAnonymousException,
                    self.run_flo, [t])


if __name__ == '__main__':
    tt.main()

