# Functions assisting with Python 2/3 and Tornado 4/5 compatibility

from tornado import concurrent as cc


def future_set_exc_info(fut, exc_info):
    """
    Sets the exception information of a `Future`, including the traceback.
    """
    try:
        # tornado >= 5
        cc.future_set_exc_info(fut, exc_info)
    except AttributeError:
        # tornado < 5
        fut.set_exc_info(exc_info)
