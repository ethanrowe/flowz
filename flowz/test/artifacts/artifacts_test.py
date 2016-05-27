from collections import OrderedDict
from concurrent import futures

from nose import tools
from tornado import gen
from tornado import testing as tt
import tornado.concurrent

from flowz.artifacts import (ExtantArtifact, DerivedArtifact, ThreadedDerivedArtifact,
                             WrappedArtifact, TransformedArtifact, KeyedArtifact,
                             maybe_artifact)

from ..channels.util import raises_channel_done


class ArtifactsTest(tt.AsyncTestCase):
    NAME = "Fooble"
    NUM_ARR = [1, 2, 3, 4, 5]
    NUM_DICT = {1: "one", 2: "two", 3: "three", 4: "four", 5: "five"}
    NUM_ORDERED_DICT = OrderedDict([(i, NUM_DICT[i]) for i in NUM_ARR])
    NUM_REVERSED_DICT = OrderedDict([(i, NUM_DICT[i]) for i in reversed(NUM_ARR)])

    @staticmethod
    @gen.coroutine
    def get_ordered_dict():
        raise gen.Return(ArtifactsTest.NUM_ORDERED_DICT)

    @staticmethod
    def derive_ordered_dict(num_arr, num_dict):
        return OrderedDict([(i, num_dict[i]) for i in num_arr])

    @staticmethod
    def transform_reversed_dict(orig_dict):
        return OrderedDict([(i, orig_dict[i]) for i in reversed(orig_dict.keys())])

    @staticmethod
    @gen.coroutine
    def get_foo():
        raise gen.Return('foo')

    @staticmethod
    def derive_foo():
        return 'foo'

    @staticmethod
    def derive_value(key, dict_):
        return dict_[key]

    @staticmethod
    def derive_key(dict_, value):
        for (k, v) in dict_.iteritems():
            if v == value:
                return k
        return None

    @staticmethod
    @gen.coroutine
    def battery(artifact_maker, exp_value, first_exists):
        artifact = artifact_maker()
        tools.assert_true(ArtifactsTest.NAME in str(artifact))
        tools.assert_equal(artifact.exists(), first_exists)
        tools.assert_true(artifact.ensure())

        value = yield artifact.get()

        tools.assert_equal(value, exp_value)
        tools.assert_true(artifact.exists())
        tools.assert_true(artifact.ensure())

        raise gen.Return(True)

    @tt.gen_test
    def test_extant_artifact(self):
        func = lambda: ExtantArtifact(self.get_ordered_dict, name=self.NAME)
        yield self.battery(func, self.NUM_ORDERED_DICT, True)

    @tt.gen_test
    def test_derived_artifact(self):
        func = lambda: DerivedArtifact(self.derive_ordered_dict, self.NUM_ARR,
                                       self.NUM_DICT, name=self.NAME)
        yield self.battery(func, self.NUM_ORDERED_DICT, False)

    @tt.gen_test
    def test_threaded_derived_artifact(self):
        executor = futures.ThreadPoolExecutor(1)
        func = lambda: ThreadedDerivedArtifact(executor, self.derive_ordered_dict,
                                               self.NUM_ARR, self.NUM_DICT, name=self.NAME)
        yield self.battery(func, self.NUM_ORDERED_DICT, False)

    @tt.gen_test
    def test_wrapped_artifact(self):
        func = lambda: WrappedArtifact(DerivedArtifact(self.derive_ordered_dict,
                                                       self.NUM_ARR, self.NUM_DICT),
                                       name=self.NAME)
        yield self.battery(func, self.NUM_ORDERED_DICT, False)

    @tt.gen_test
    def test_transformed_artifact(self):
        # Try with an ExtantArtifact
        func = lambda: TransformedArtifact(ExtantArtifact(self.get_ordered_dict),
                                           self.transform_reversed_dict, name=self.NAME)
        yield self.battery(func, self.NUM_REVERSED_DICT, True)

        # Try with a DerivedArtifact
        func = lambda: TransformedArtifact(DerivedArtifact(self.derive_ordered_dict,
                                                           self.NUM_ARR, self.NUM_DICT),
                                       self.transform_reversed_dict, name=self.NAME)
        yield self.battery(func, self.NUM_REVERSED_DICT, False)

    @tt.gen_test
    def test_keyed_artifact(self):
        key = 1
        func = lambda: KeyedArtifact(key,
                                     DerivedArtifact(self.derive_value, key, self.NUM_DICT),
                                     name=self.NAME)
        yield self.battery(func, 'one', False)

        artifact = func()
        tools.assert_equal(artifact[0], key)
        tools.assert_equal(artifact[1], artifact)
        tools.assert_equal(artifact['key'], key)
        tools.assert_raises(KeyError, artifact.__getitem__, 'spaz')

        for (a,b) in zip((key, artifact), iter(artifact)):
            tools.assert_equal(a, b)

    @tt.gen_test
    def test_keyed_artifact_transform(self):
        key = 1
        artifact = KeyedArtifact(key, DerivedArtifact(self.derive_value, key, self.NUM_DICT))
        artifact2 = artifact.transform(self.derive_key, self.NUM_DICT)
        key2 = yield artifact2.get()
        tools.assert_equal(key, key2)
        tools.assert_is_instance(artifact2, KeyedArtifact)

    @tt.gen_test
    def test_keyed_artifact_threaded_transform(self):
        executor = futures.ThreadPoolExecutor(1)
        key = 1
        artifact = KeyedArtifact(key, DerivedArtifact(self.derive_value, key, self.NUM_DICT))
        artifact2 = artifact.threaded_transform(executor, self.derive_key, self.NUM_DICT)
        key2 = yield artifact2.get()
        tools.assert_equal(key, key2)
        tools.assert_is_instance(artifact2, KeyedArtifact)

    @tt.gen_test
    def test_maybe_artifact(self):
        # prove that both artifacts and non-artifacts result in futures
        key = 1
        artifact = DerivedArtifact(self.derive_value, key, self.NUM_DICT)
        future1 = maybe_artifact(artifact)
        tools.assert_is_instance(future1, tornado.concurrent.Future)

        future2 = maybe_artifact('one')
        tools.assert_is_instance(future2, tornado.concurrent.Future)

        val1 = yield future1
        val2 = yield future2
        tools.assert_equal(val1, val2)

        # Make sure that just having a "get" function isn't enough to be an artifact!
        dict_ = {1: 'one'}
        tools.assert_true(hasattr(dict_, 'get'))
        future3 = maybe_artifact(dict_)
        val3 = yield future3
        tools.assert_equal(val3, dict_)

    # ****** End with helper methods on AbstractArtifact ******

    @tt.gen_test
    def test_as_channel(self):
        artifact = ExtantArtifact(self.get_foo)
        channel = artifact.as_channel()
        result = yield channel.start()
        tools.assert_true(result)

        artifact2 = yield channel.next()
        tools.assert_equal(artifact, artifact2)

        val = yield artifact2.get()
        tools.assert_equal(val, 'foo')

        yield raises_channel_done(channel)

    @tt.gen_test
    def test_value_channel(self):
        artifact = ExtantArtifact(self.get_foo)
        channel = artifact.value_channel()
        result = yield channel.start()
        tools.assert_true(result)

        val = yield channel.next()
        tools.assert_equal(val, 'foo')

        yield raises_channel_done(channel)

    @tt.gen_test
    def test_ensure_channel(self):
        artifact = DerivedArtifact(self.derive_foo)
        tools.assert_false(artifact.exists())
        channel = artifact.ensure_channel()
        result = yield channel.start()
        tools.assert_true(result)

        ensured = yield channel.next()
        tools.assert_true(ensured)

        tools.assert_true(artifact.exists())

        yield raises_channel_done(channel)
