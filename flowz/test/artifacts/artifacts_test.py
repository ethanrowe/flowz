from collections import OrderedDict
from concurrent import futures

from nose import tools
from tornado import gen
from tornado import testing as tt
import tornado.concurrent

from flowz.artifacts import (ExtantArtifact, DerivedArtifact, ThreadedDerivedArtifact,
                             WrappedArtifact, TransformedArtifact, KeyedArtifact,
                             maybe_artifact)
from flowz.channels import ChannelDone


class ArtifactsTest(tt.AsyncTestCase):
    NUM_ARR = [1, 2, 3, 4, 5]
    NUM_DICT = {1: "one", 2: "two", 3: "three", 4: "four", 5: "five"}
    NUM_ORDERED_DICT = OrderedDict([(i, NUM_DICT[i]) for i in NUM_ARR])
    NUM_REVERSED_DICT = OrderedDict([(i, NUM_DICT[i]) for i in reversed(NUM_ARR)])

    @tt.gen_test
    def test_extant_artifact(self):
        @gen.coroutine
        def getter():
            raise gen.Return(self.NUM_ORDERED_DICT)

        artifact = ExtantArtifact(getter, name="ExtantArtifactTester")

        tools.assert_true("ExtantArtifactTester" in str(artifact))
        tools.assert_true(artifact.exists())
        tools.assert_true(artifact.ensure())

        value = yield artifact.get()

        tools.assert_equal(value, self.NUM_ORDERED_DICT)

    @tt.gen_test
    def test_derived_artifact(self):
        def deriver(num_arr, num_dict):
            return OrderedDict([(i, num_dict[i]) for i in num_arr])

        artifact = DerivedArtifact(deriver, self.NUM_ARR, self.NUM_DICT)

        tools.assert_false(artifact.exists())

        value = yield artifact.get()

        tools.assert_equal(value, self.NUM_ORDERED_DICT)
        tools.assert_true(artifact.exists())
        tools.assert_true(artifact.ensure())

        # Now call with a slightly different order, calling ensure() before get()
        artifact = DerivedArtifact(deriver, self.NUM_ARR, self.NUM_DICT)

        tools.assert_false(artifact.exists())
        ensured = yield artifact.ensure()
        tools.assert_true(ensured)

        value = yield artifact.get()

        tools.assert_equal(value, self.NUM_ORDERED_DICT)
        tools.assert_true(artifact.exists())

    @tt.gen_test
    def test_threaded_derived_artifact(self):
        def deriver(num_arr, num_dict):
            return OrderedDict([(i, num_dict[i]) for i in num_arr])

        executor = futures.ThreadPoolExecutor(1)

        artifact = ThreadedDerivedArtifact(executor, deriver, self.NUM_ARR, self.NUM_DICT)

        tools.assert_false(artifact.exists())

        value = yield artifact.get()

        tools.assert_equal(value, self.NUM_ORDERED_DICT)
        tools.assert_true(artifact.exists())
        tools.assert_true(artifact.ensure())

        # Now call with a slightly different order, calling ensure() before get()
        artifact = ThreadedDerivedArtifact(executor, deriver, self.NUM_ARR, self.NUM_DICT)

        tools.assert_false(artifact.exists())
        ensured = yield artifact.ensure()
        tools.assert_true(ensured)

        value = yield artifact.get()

        tools.assert_equal(value, self.NUM_ORDERED_DICT)
        tools.assert_true(artifact.exists())

    @tt.gen_test
    def test_wrapped_artifact(self):
        def deriver(num_arr, num_dict):
            return OrderedDict([(i, num_dict[i]) for i in num_arr])

        artifact = WrappedArtifact(DerivedArtifact(deriver, self.NUM_ARR, self.NUM_DICT))

        tools.assert_false(artifact.exists())
        value = yield artifact.get()
        tools.assert_equal(value, self.NUM_ORDERED_DICT)
        tools.assert_true(artifact.exists())
        tools.assert_true(artifact.ensure())

    @tt.gen_test
    def test_transformed_artifact(self):
        @gen.coroutine
        def getter():
            raise gen.Return(self.NUM_ORDERED_DICT)

        def deriver(num_arr, num_dict):
            return OrderedDict([(i, num_dict[i]) for i in num_arr])

        def transformer(orig_dict):
            return OrderedDict([(i, orig_dict[i]) for i in reversed(orig_dict.keys())])

        # Try with an ExtantArtifact
        artifact = TransformedArtifact(ExtantArtifact(getter), transformer)

        tools.assert_true(artifact.exists())
        tools.assert_true(artifact.ensure())

        value = yield artifact.get()

        tools.assert_equal(value, self.NUM_REVERSED_DICT)

        # Try with a DerivedArtifact
        artifact = TransformedArtifact(DerivedArtifact(deriver, self.NUM_ARR, self.NUM_DICT),
                                       transformer)

        tools.assert_false(artifact.exists())

        value = yield artifact.get()

        tools.assert_equal(value, self.NUM_REVERSED_DICT)
        tools.assert_true(artifact.exists())
        tools.assert_true(artifact.ensure())

    @tt.gen_test
    def test_keyed_artifact(self):
        def deriver(key, num_dict):
            return num_dict[key]

        key = 1
        artifact = KeyedArtifact(key, DerivedArtifact(deriver, key, self.NUM_DICT))

        tools.assert_equal(artifact[0], key)
        tools.assert_equal(artifact[1], artifact)
        tools.assert_equal(artifact['key'], key)
        tools.assert_raises(KeyError, artifact.__getitem__, 'spaz')

        for (a,b) in zip((key, artifact), iter(artifact)):
            tools.assert_equal(a, b)

        tools.assert_false(artifact.exists())

        value = yield artifact.get()

        tools.assert_equal(value, 'one')
        tools.assert_true(artifact.exists())
        tools.assert_true(artifact.ensure())

    @tt.gen_test
    def test_keyed_artifact_transform(self):
        def deriver(key, num_dict):
            return num_dict[key]

        def rderiver(num_dict, val):
            for (k, v) in num_dict.iteritems():
                if v == val:
                    return k
            return None

        key = 1
        artifact = KeyedArtifact(key, DerivedArtifact(deriver, key, self.NUM_DICT))
        artifact2 = artifact.transform(rderiver, self.NUM_DICT)

        key2 = yield artifact2.get()
        tools.assert_equal(key, key2)

    @tt.gen_test
    def test_keyed_artifact_threaded_transform(self):
        def deriver(key, num_dict):
            return num_dict[key]

        def rderiver(num_dict, val):
            for (k, v) in num_dict.iteritems():
                if v == val:
                    return k
            return None

        executor = futures.ThreadPoolExecutor(1)

        key = 1
        artifact = KeyedArtifact(key, DerivedArtifact(deriver, key, self.NUM_DICT))
        artifact2 = artifact.threaded_transform(executor, rderiver, self.NUM_DICT)

        key2 = yield artifact2.get()
        tools.assert_equal(key, key2)

    @tt.gen_test
    def test_maybe_artifact(self):
        def deriver(key, num_dict):
            return num_dict[key]

        # prove that both artifacts and non-artifacts result in futures
        key = 1
        artifact = DerivedArtifact(deriver, key, self.NUM_DICT)
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
        @gen.coroutine
        def getter():
            raise gen.Return('foo')

        artifact = ExtantArtifact(getter)
        channel = artifact.as_channel()
        result = yield channel.start()
        tools.assert_true(result)

        artifact2 = yield channel.next()
        tools.assert_equal(artifact, artifact2)

        val = yield artifact2.get()
        tools.assert_equal(val, 'foo')

        try:
            artifact3 = yield channel.next()
            tools.assert_true(False, "Channel unexpectedly had an extra artifact")
        except ChannelDone:
            pass
        tools.assert_true(channel.done())

    @tt.gen_test
    def test_value_channel(self):
        @gen.coroutine
        def getter():
            raise gen.Return('foo')

        artifact = ExtantArtifact(getter)
        channel = artifact.value_channel()
        result = yield channel.start()
        tools.assert_true(result)

        val = yield channel.next()
        tools.assert_equal(val, 'foo')

        try:
            val2 = yield channel.next()
            tools.assert_true(False, "Channel unexpectedly had an extra value")
        except ChannelDone:
            pass
        tools.assert_true(channel.done())

    @tt.gen_test
    def test_ensure_channel(self):
        def deriver():
            return 'foo'

        artifact = DerivedArtifact(deriver)
        tools.assert_false(artifact.exists())
        channel = artifact.ensure_channel()
        result = yield channel.start()
        tools.assert_true(result)

        ensured = yield channel.next()
        tools.assert_true(ensured)

        tools.assert_true(artifact.exists())

        try:
            ensured2 = yield channel.next()
            tools.assert_true(False, "Channel unexpectedly had an extra value")
        except ChannelDone:
            pass
        tools.assert_true(channel.done())
