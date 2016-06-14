from concurrent import futures
import logging

import mock
from nose import tools
from tornado import gen
from tornado import testing as tt

from flowz.artifacts import (ExtantArtifact, DerivedArtifact, ThreadedDerivedArtifact,
                             TransformedArtifact)


class ExceptionLoggingTest(tt.AsyncTestCase):
    # Possible getter/deriver/transform functions

    @classmethod
    def exc_raise(cls, obj=None):
        return 3 / 0

    @classmethod
    @gen.coroutine
    def exc_raise_co(cls):
        raise gen.Return(cls.exc_raise())

    @staticmethod
    @gen.coroutine
    def check_for_exception(artifact_maker):
        """
        Checks that getting an artifact raises and logs an exception
        @param artifact_maker: a callable to build the artifact

        NOTE: This has a hack until I can figure out how to find out if a function is called
        on a mock without testing its parameters.
        """
        artifact = artifact_maker()
        artifact.logger = mock.Mock()
        artifact.logger.exception.side_effect = lambda x: artifact.logger.exception.phoo("pham")

        try:
            value = yield artifact.get()
            tools.assert_true(False, "Expected exception not thrown")
        except ZeroDivisionError:
            artifact.logger.exception.phoo.assert_called_with("pham")
        except:
            tools.assert_true(False, "Different exception raised than expected")

        raise gen.Return(True)

    @tt.gen_test
    def test_extant_artifact(self):
        yield self.check_for_exception(lambda: ExtantArtifact(self.exc_raise_co))

    @tt.gen_test
    def test_derived_artifact(self):
        yield self.check_for_exception(lambda: DerivedArtifact(self.exc_raise))

    @tt.gen_test
    def test_threaded_derived_artifact(self):
        executor = futures.ThreadPoolExecutor(1)
        yield self.check_for_exception(lambda: ThreadedDerivedArtifact(executor, self.exc_raise))

    @tt.gen_test
    def test_transformed_artifact(self):
        yield self.check_for_exception(lambda: TransformedArtifact("foo", self.exc_raise))
