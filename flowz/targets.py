"""
Target interface and core classes for operations in a Flo.
"""

import itertools

from tornado import gen

class Target(object):
    """
    The base class and minimal interface for a "target" within a flo.

    A `Target` represents a node of work within a workflow graph.  While workflow
    graphs are easily done in tornado with coroutines, that requires thinking
    about thinks asynchronously.  The `Target` concept allows a user to write
    synchronous code, bundle them into various kinds of `Target` objects, and
    have the asynchronous aspects handled for them.

    A `Target` is thought of as "returning" a value; in principle, that value
    is the output of whatever operation or computation it is that a given
    target node represents, and specifically is the value returned by the
    future underlying the target's `future()` method.

    Basic interface:
    - A `Target` has a `future()` method that should return a tornado-comp.
      future, the result of which is the result of the target's operation.

    Specific to this implementation and important for subclasses:
    - A `Target` has a `start()` method that is a tornado-comp. coroutine,
      which backs the `future()` method.

    Users of the `Target` (namely, a `flowz.app.Flo` object, or other `Target`
    objects) should only deal with `future()`; the presence of `start` is
    specific to this class.  The `future()` in this case is responsible for
    doing `start()` once and only once; the resulting future is remembered and
    is the subsequent result of any further calls to  `future()`.

    A subclass of `Target` should override `start()` and leave `future()` alone.
    The `start()` method should return a tornado-comp. future, which is most
    conveniently done if it's a tornado-comp. coroutine.
    """
    __future__ = None

    @gen.coroutine
    def start(self):
        raise NotImplementedError("You need to implement start yourself.")

    def future(self):
        if self.__future__ is None:
            self.__future__ = self.start()
        return self.__future__


class CallTarget(Target):
    """
    A `Target` that will return the result of its `targetcallable`.

    Given a parameterless callable `targetcallable`, this `Target`'s future
    returns the result of that `targetcallable`.

    Bundling it in this means that it can be used in the asynchronous `Flo`
    framework, and the `targetcallable` will only be invoked once within the
    scope of its containing `CallTarget`.

        target = CallTarget(lambda: db.some_expensive_fetch())
        a = target.future()
        ...
        b = target.future()
        # This is true.
        assert(a == b)
        ...
        # `r` will get the result of `db.some_expensive_fetch()`, if done
        # in a tornado coroutine.
        r = yield target.future()

    Note that as in all things with tornado, if you call something that
    immediately blocks on IO, you're performing traditional synchronous, blocking
    I/O.  In this example, you would want `db.some_expensive_fetch` to run
    on a thread pool executor or something that would allow it to be non-blocking
    from the perspective of the tornado ioloop.

    So, in principle, for things like expensive DB queries, fetches of assets from
    S3, etc., we would want supporting structures to facilitate the asynchronous
    I/O portion of things.  After which, using them with `CallTarget` should be
    pretty natural.
    """
    def __init__(self, targetcallable):
        self.target = targetcallable

    @gen.coroutine
    def start(self):
        r = self.target()
        raise gen.Return(r)


class FuncTarget(Target):
    """
    A `Target` that asynchronously retrieves a function's parameters.

    Given a function and some number of positional and keyword params,
    will identify and asynchronously gather the values of any params that
    appear to be `Targets`, replacing those params with their target results
    prior to passing along to the wrapped callable.  Params that do not appear
    to be `Targets` will be preserved when passed to the wrapped callable.

    This is best explained via a simple thought experiment.

    Suppose you have some function `transform`, which accepts CSV text
    and converts it to some native form (like a pandas dataframe).

    Suppose that you have an object in S3 that holds the CSV content you want
    to transform.

    Suppose further that you have some function `fetch_s3_content` that will
    perform the retrieval of S3 content for you in an asynchronous manner (like
    on a thread pool executor).

    You can have a target that applies the transformation to that content thus:

        s3_content = CallTarget(lambda: s3_async.fetch(your_object_key))
        transformation = FuncTarget(transform, s3_content)

    Suppose still farther that you have an additional function `delta` that
    calculates the differences between data structures produced by your
    `transform` function.  Suppose it uses keyword parameters.

    You could have a target that contains the different between multiple objects
    like so

        def s3_transformer(object_key):
            return FuncTarget(transform, CallTarget(
                lambda: s3_async.fetch(object_key)))

        difference = FuncTarget(delta, a=s3_transformer(object_key_a),
            b=s3_transformer(object_key_b),
            c=s3_transformer(object_key_c))

    Now suppose the `delta` function takes an additional keyword parameter that
    tunes its behaviors; the value is a simple float, and you don't need to do
    any concurrent stuff to know the desired value.  You can mix and match; if the
    parameter doesn't look like a `Target`, it will be passed through unchanged.

    So our `difference` call might look like this instead:

        difference = FuncTarget(delta, some_tuning_param=0.473,
            a=s3_transformer(object_key_a),
            b=s3_transformer(object_key_b),
            c=s3_transformer(object_key_c))

    So the guy writing business logic like `transform` and `delta` can focus
    on the business logic, and the gal integrating stuff into a live workflow
    can focus on the gathering of inputs, handling the asynchronous stuff.
    """
    def __init__(self, func, *argets, **kwargets):
        self.func = func
        self.argets = argets
        self.kwargets = kwargets


    @gen.coroutine
    def start(self):
        args, kwargs = yield self.gather_arguments()
        r = self.func(*args, **kwargs)
        raise gen.Return(r)


    @gen.coroutine
    def gather_arguments(self):
        # To impose a particular order on the keywords, we'll use the
        # natural ordering of the keys.
        a = self.argets
        kw = self.kwargets

        # The total set of parameter values, flattened.
        inputs = list(itertools.chain(a, kw.values()))

        # The subset of those parameter values that are targets
        # (they have a 'future')
        futures = dict((i, arg.future())
                for i, arg in enumerate(inputs)
                if hasattr(arg, 'future'))

        if len(futures):
            # Wait on the futures.
            futures = yield futures

            # Merge them back into the inputs.
            for i, result in futures.items():
                inputs[i] = result
            kw = dict(zip(kw.keys(), inputs[len(a):]))
            a = inputs[:len(a)]

        # And return the assembled parameters.
        raise gen.Return((a, kw))


class FutureTarget(FuncTarget):
    """
    A target that asynchronously retrieves parameters and a final result.

    This extends `FuncTarget`, and is consistent in the handling of parameters
    (both positional and keyword).  However, the callable (be it a function or
    a class or whatever else) is expected to return a target (something with
    a `future` method).

    The `FutureTarget` will thus asynchronously gather parameters, like
    `FuncTarget`, and will pass them to the callable.  It will then
    asynchronously invoke the target returned by the callable.

    The result of the `FutureTarget` is the result of that future.

    Think of it like this: it's `FuncTarget`, but for `Target` objects instead
    of functions.
    """
    @gen.coroutine
    def start(self):
        r = yield super(FutureTarget, self).start()
        r = yield r.future()
        raise gen.Return(r)

