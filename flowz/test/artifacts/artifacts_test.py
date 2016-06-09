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

    # Possible getter/deriver/transform functions

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
    def battery(artifact_maker, exp_value, exists_pre_get):
        """
        A batter of tests to run against a particular artifact type
        @param artifact_maker: a callable to build the artifact
        @param exp_value: the expected value of getting the artifact
        @param exists_pre_get: the expect value of calling exists() before calling get()
        """
        artifact = artifact_maker()
        tools.assert_true(ArtifactsTest.NAME in str(artifact))
        tools.assert_equal(artifact.exists(), exists_pre_get)
        tools.assert_true(artifact.ensure())

        value = yield artifact.get()

        tools.assert_equal(value, exp_value)
        tools.assert_true(artifact.exists())
        tools.assert_true(artifact.ensure())

        @gen.coroutine
        def check_channel(channel, exp_value):
            """
            Validate a channel with one artifact in it
            @param channel: the channel
            @param exp_value: the expected value of the entry in the channel
            """
            result = yield channel.start()
            tools.assert_true(result)

            obj = yield channel.next()
            # the object might be an artifact or a direct value
            val = yield maybe_artifact(obj)
            tools.assert_equal(val, exp_value)

            yield raises_channel_done(channel)
            raise gen.Return(True)

        yield check_channel(artifact_maker().as_channel(), exp_value)
        yield check_channel(artifact_maker().value_channel(), exp_value)
        yield check_channel(artifact_maker().ensure_channel(), True)

        raise gen.Return(True)

    @tt.gen_test
    def test_extant_artifact(self):
        maker = lambda: ExtantArtifact(self.get_ordered_dict, name=self.NAME)
        yield self.battery(maker, self.NUM_ORDERED_DICT, True)

    @tt.gen_test
    def test_derived_artifact(self):
        maker = lambda: DerivedArtifact(self.derive_ordered_dict, self.NUM_ARR,
                                     self.NUM_DICT, name=self.NAME)
        yield self.battery(maker, self.NUM_ORDERED_DICT, False)

    @tt.gen_test
    def test_threaded_derived_artifact(self):
        executor = futures.ThreadPoolExecutor(1)
        maker = lambda: ThreadedDerivedArtifact(executor, self.derive_ordered_dict,
                                                self.NUM_ARR, self.NUM_DICT, name=self.NAME)
        result = yield self.battery(maker, self.NUM_ORDERED_DICT, False)

    @tt.gen_test
    def test_wrapped_artifact(self):
        maker = lambda: WrappedArtifact(DerivedArtifact(self.derive_ordered_dict,
                                                        self.NUM_ARR, self.NUM_DICT),
                                        name=self.NAME)
        yield self.battery(maker, self.NUM_ORDERED_DICT, False)

    @tt.gen_test
    def test_wrapped_artifact_getattr(self):
        artifact = WrappedArtifact(DerivedArtifact(self.derive_ordered_dict,
                                                   self.NUM_ARR, self.NUM_DICT),
                                   name=self.NAME)

        # in a normal situation, getting attributes should work fine, passing the call
        # onto the underlying value...
        tools.assert_equal(self.derive_ordered_dict, getattr(artifact, 'deriver'))
        # ...and throwing AttributeError if it didn't have the attribute
        tools.assert_raises(AttributeError, getattr, artifact, 'phamble')

        # If you had not yet set a value attribute on the artifact, though...
        delattr(artifact, 'value')
        # ...this used to infinitely recurse until Python complained.
        # But now it should return a proper AttributeError
        tools.assert_raises(AttributeError, getattr, artifact, 'deriver')

    @tt.gen_test
    def test_transformed_artifact(self):
        # Try with an ExtantArtifact
        maker = lambda: TransformedArtifact(ExtantArtifact(self.get_ordered_dict),
                                            self.transform_reversed_dict, name=self.NAME)
        yield self.battery(maker, self.NUM_REVERSED_DICT, True)

        # Try with a DerivedArtifact
        maker = lambda: TransformedArtifact(DerivedArtifact(self.derive_ordered_dict,
                                                            self.NUM_ARR, self.NUM_DICT),
                                            self.transform_reversed_dict, name=self.NAME)
        yield self.battery(maker, self.NUM_REVERSED_DICT, False)

    @tt.gen_test
    def test_keyed_artifact(self):
        key = 1
        maker = lambda: KeyedArtifact(key,
                                      DerivedArtifact(self.derive_value, key, self.NUM_DICT),
                                      name=self.NAME)
        yield self.battery(maker, 'one', False)

        artifact = maker()
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
