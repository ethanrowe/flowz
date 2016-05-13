import mock
from nose import tools

from flowz import util

def mockfunc():
    m = mock.Mock(name='MockFunction')
    m.results = []
    m.results_for = lambda i: getattr(m, 'result%d' % i)
    def call(*a, **kw):
        r = m.results_for(len(m.results))
        m.results.append(r)
        return r
    m.side_effect = call
    return m


def test_function_pair():
    first = mockfunc()
    trailing = mockfunc()
    calls = 5

    args = [((mock.Mock(), mock.Mock()), {'a': mock.Mock(), 'b': mock.Mock()})
            for i in range(calls)]

    wrapper = util.LastResult(trailing, first)
    expected_results = (
            [first.results_for(0)] +
                [trailing.results_for(i) for i in range(calls - 1)])

    results = [wrapper(*a, **kw) for a, kw in args]

    # It gets the right results.
    yield tools.assert_equal, expected_results, results

    # The first guy is called with just the given args.
    yield lambda: first.assert_called_once_with(*args[0][0], **args[0][1])

    # The trailing guy is called the remaining times, each time with
    # the previous result appended to positional params.
    for i, (a, kw) in enumerate(args[1:]):
        a += (results[i],)
        yield lambda pair, idx: tools.assert_equal(
                pair, trailing.call_args_list[idx]), (a, kw), i


def test_single_function():
    func = mockfunc()

    calls = 6

    args = [((mock.Mock(),),
                {'eks': mock.Mock(), 'uie': mock.Mock(), 'zi': mock.Mock()})
            for i in range(calls)]

    expected_results = [func.results_for(i) for i in range(calls)]

    wrapper = util.LastResult(func)

    results = [wrapper(*a, **kw) for a, kw in args]

    yield tools.assert_equal, expected_results, results

    # First called with the special NO_VALUE object.
    for i, ((a, kw), arg) in enumerate(zip(
            args, [util.NO_VALUE] + expected_results[:-1])):
        a += (arg,)
        yield lambda pair, idx: tools.assert_equal(
                pair, func.call_args_list[idx]), (a, kw), i



