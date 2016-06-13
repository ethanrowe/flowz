from __future__ import absolute_import, division, print_function, with_statement

import functools
import logging

from tornado import stack_context, gen


def logging_coroutine(func, logger=None):
    """
    A wrapper to replace :ref:`gen.coroutine` that will log exceptions from the
    underlying coroutine prior to the normal mechanism packaging them in the future.
    """
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        try:
            result = func(*args, **kwargs)
        except (gen.Return, StopIteration) as e:
            raise e
        except Exception as e:
            if logger:
                logger.exception()
            else:
                logging.exception()
            raise e
        return result

    @gen.coroutine
    @functools.wraps(func)
    def wrapper2(*args, **kwargs):
        return wrapper(*args, **kwargs)

    return wrapper2
