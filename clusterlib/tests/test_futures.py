import os
from time import sleep

from nose.tools import assert_equal
from nose.tools import assert_raises
from nose.tools import assert_in
from nose.tools import assert_true
from nose.tools import assert_false

from clusterlib.futures import ClusterExecutor
from clusterlib.futures import ClusterCancelledError
from clusterlib.futures import AtomicMarker
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


def test_atomic_markers():
    with TemporaryDirectory() as test_folder:
        with AtomicMarker(test_folder, 'marker') as marker:
            assert_true(marker.isset())

            # Trying to set the original marker twice ignores
            # the request
            assert_false(marker.set())

            # Create a different marker in the same folder but with a
            # different identifier
            marker2 = AtomicMarker(test_folder, 'other_marker',
                                   raise_if_exists=True)
            assert_false(marker2.isset())
            assert_true(marker2.set())
            assert_true(marker2.unset())
            assert_false(marker2.unset())

            # Create a new marker with the original id, in the same folder
            marker3 = AtomicMarker(test_folder, 'marker',
                                   raise_if_exists=True)

            assert_true(marker3.isset())
            assert_raises(OSError, marker3.set)
            assert_true(marker.isset())

        # Outside the with block the marker is unset
        assert_false(marker.isset())
        assert_false(marker3.isset())

        # It's now possible to set it with marker3
        assert_true(marker3.set())
        assert_true(marker.isset())
        assert_true(marker3.isset())

        # It's still not possible to set it twice
        assert_raises(OSError, marker3.set)


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
        with ClusterExecutor(folder=cluster_folder, poll_interval=1) as e:
            results = e.map(_increment, range(10))
            assert_equal(list(results), list(range(1, 11)))

            # When the callable raises an exception, only the first exception
            # is raised
            assert_raises(ValueError,
                          e.map, _raise_exc,
                          [ValueError(), AttributeError()])
            assert_raises(AttributeError,
                          e.map, _raise_exc,
                          [AttributeError(), ValueError()])

            # When the callable raises an exception, only the first exception
            # is raised
            assert_raises(CustomException,
                          e.map, _raise_exc,
                          [CustomException(), ValueError()])


@skip_if_no_backend
def test_executor_submit():
    with TemporaryDirectory() as test_folder:
        cluster_folder = os.path.join(test_folder, 'clusterlib')
        with ClusterExecutor(folder=cluster_folder, poll_interval=1) as e:
            f1 = e.submit(_increment, 1)
            f2 = e.submit(_increment, 2)

            # Submit can pass kwargs to the callable
            f3 = e.submit(_increment, 3, step=-1)

            # Check that it's possible to asyncrhonously check the state of
            # the computation without blocking
            for f in [f1, f2, f3]:
                assert_in(f.running(), [True, False])
                assert_in(f.done(), [True, False])
                assert_false(f.cancelled())

            # Check the result (block untill there are available)
            assert_equal(f1.result(), 2)
            assert_equal(f2.result(), 3)
            assert_equal(f3.result(), 2)

            # After having blocked to get the results, the future is done
            for f in [f1, f2, f3]:
                assert_true(f.done())
                assert_false(f.running())
                assert_false(f.cancelled())
                assert_equal(f.exception(), None)

            # It is still possible to collect the results several times
            assert_equal(f1.result(), 2)
            assert_equal(f2.result(), 3)
            assert_equal(f3.result(), 2)

            # It is not possible to cancel done tasks
            for f in [f1, f2, f3]:
                assert_true(f.done())
                assert_false(f.cancel())
                assert_false(f.cancelled())


@skip_if_no_backend
def test_executor_cancel():
    with TemporaryDirectory() as test_folder:
        cluster_folder = os.path.join(test_folder, 'clusterlib')
        with ClusterExecutor(folder=cluster_folder, poll_interval=1) as e:
            f1 = e.submit(sleep, 100)
            f2 = e.submit(sleep, 200)

            for f in [f1, f2]:
                assert_false(f.done())
                assert_false(f.cancelled())

            # Cancel the second long running jobs
            assert_true(f2.cancel(interrupt_running=True))
            assert_true(f2.cancelled())
            assert_false(f2.running())
            assert_false(f2.done())

            # Accessiong the results or exception from a cancelled job is
            # forbidden by raising a specific exception
            assert_raises(ClusterCancelledError, f2.result)
            assert_raises(ClusterCancelledError, f2.exception)

            # The first job is still running
            assert_false(f2.done())
            assert_false(f2.cancelled())

            # Cancel it as well
            assert_true(f1.cancel(interrupt_running=True))
            assert_false(f1.done())
            assert_true(f1.cancelled())
            assert_raises(ClusterCancelledError, f1.result)
            assert_raises(ClusterCancelledError, f1.exception)
