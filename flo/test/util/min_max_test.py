import functools
import sys

import mock
from nose import tools

from flowz import util

def min_lt(f):
    @functools.wraps(f)
    def t():
        v = f()
        print v
        tools.assert_true(util.MINIMUM < v)
        tools.assert_true(util.MINIMUM <= v)
        tools.assert_true(util.MINIMUM != v)
        tools.assert_false(util.MINIMUM == v)
        tools.assert_false(util.MINIMUM >= v)
        tools.assert_false(util.MINIMUM > v)
    return t


def min_eq(f):
    @functools.wraps(f)
    def t():
        v = f()
        print v
        tools.assert_true(util.MINIMUM == v)
        tools.assert_true(util.MINIMUM <= v)
        tools.assert_true(util.MINIMUM >= v)
        tools.assert_false(util.MINIMUM < v)
        tools.assert_false(util.MINIMUM > v)
        tools.assert_false(util.MINIMUM != v)
    return t


def max_gt(f):
    @functools.wraps(f)
    def t():
        v = f()
        print v
        tools.assert_true(util.MAXIMUM > v)
        tools.assert_true(util.MAXIMUM >= v)
        tools.assert_true(util.MAXIMUM != v)
        tools.assert_false(util.MAXIMUM == v)
        tools.assert_false(util.MAXIMUM <= v)
        tools.assert_false(util.MAXIMUM < v)
    return t


def max_eq(f):
    @functools.wraps(f)
    def t():
        v = f()
        print v
        tools.assert_true(util.MAXIMUM == v)
        tools.assert_true(util.MAXIMUM >= v)
        tools.assert_true(util.MAXIMUM <= v)
        tools.assert_false(util.MAXIMUM != v)
        tools.assert_false(util.MAXIMUM > v)
        tools.assert_false(util.MAXIMUM < v)
    return t


@min_lt
def test_min_lt_none():
    return None


@min_lt
def test_min_lt_chr0():
    return chr(0)


@min_lt
def test_min_lt_int():
    return -1 * (sys.maxsize - 1)


@min_lt
def test_min_lt_negative_infinity():
    return float('-Inf')


@min_lt
def test_min_lt_max():
    return util.MAXIMUM


@min_eq
def test_min_eq_self():
    return util.MINIMUM


@max_gt
def test_max_gt_none():
    return None


@max_gt
def test_max_gt_chr255():
    return chr(255)


@max_gt
def test_max_gt_int():
    return sys.maxsize


@max_gt
def test_max_gt_infinity():
    return float('Inf')


@max_eq
def test_max_eq_self():
    return util.MAXIMUM


@max_gt
def test_max_gt_min():
    return util.MINIMUM

