import os

from nose.tools import assert_equal
from nose.tools import assert_raises
from nose.tools import assert_in

from clusterlib.futures import ClusterExecutor
from clusterlib.testing import TemporaryDirectory, skip_if_no_backend


def _increment(a, step=1, raise_exc=None):
    """Dummy test function to call in parallel"""
    if raise_exc is not None:
        raise raise_exc
    return a + step


class CustomException(Exception):
    pass


def _raise_exc(exc):
    """Dummy function to """
    raise exc


def test_executor_folder():
    with TemporaryDirectory() as test_folder:
        cluster_folder = os.path.join(test_folder, 'clusterlib')
        with ClusterExecutor(folder=cluster_folder):
            # Check that the work folder of the executor was initialized
            assert os.path.exists(cluster_folder)


@skip_if_no_backend
def test_executor_map():
    with TemporaryDirectory() as test_folder:
        cluster_folder = os.path.join(test_folder, 'clusterlib')
        with ClusterExecutor(folder=cluster_folder) as e:
            results = e.map(_increment, range(10))
            assert_equal(list(results), list(range(1, 11)))

            # When the callable raises an exception, only the first exception
            # is raised
            assert_raises(ValueError,
                          e.map, _raise_exc, ValueError(), AttributeError())
            assert_raises(AttributeError,
                          e.map, _raise_exc, AttributeError(), ValueError())

            # When the callable raises an exception, only the first exception
            # is raised
            assert_raises(CustomException,
                          e.map, _raise_exc, CustomException(), ValueError())


@skip_if_no_backend
def test_executor_submit():
    with TemporaryDirectory() as test_folder:
        cluster_folder = os.path.join(test_folder, 'clusterlib')
        with ClusterExecutor(folder=cluster_folder) as e:
            f1 = e.submit(_increment, 1)
            f2 = e.submit(_increment, 2)

            # Submit can pass kwargs to the callable
            f3 = e.submit(_increment, 3, step=-1)

            # Check that it's possible to asyncrhonously check the state of
            # the computation without blocking
            for f in [f1, f2, f3]:
                assert_in(f.done(), [True, False])
                assert_equal(f.cancelled(), False)
                assert_equal(f.exception(), None)

            # Check the result (block untill there are available)
            assert_equal(f1.result(), 2)
            assert_equal(f2.result(), 3)
            assert_equal(f3.result(), 2)

            # After having blocked to get the results, the future is done
            for f in [f1, f2, f3]:
                assert_equal(f.done(), True)
                assert_equal(f.cancelled(), False)
                assert_equal(f.exception(), None)
