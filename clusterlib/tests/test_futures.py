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
from clusterlib.futures import CANCELLATION_SIGNALS
from clusterlib.testing import TemporaryDirectory, skip_if_no_backend


BASES_SHARED_FOLDER = '.'  # assume that the CWD is shared on the cluster


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

# Common  executor params for the tests: jobs are expected to be shorter
# than usual for the tests hence we don't use the default parameters
EXECUTOR_PARAMS = {
     'poll_interval': 1,
     'job_max_time': '300',  # XXX: in seconds for now due to to SGE limitation
     'min_memory': 300,
}


def test_atomic_markers():
    with TemporaryDirectory(dir=BASES_SHARED_FOLDER) as test_folder:
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
    with TemporaryDirectory(dir=BASES_SHARED_FOLDER) as test_folder:
        cluster_folder = os.path.join(test_folder, 'clusterlib')
        with ClusterExecutor(folder=cluster_folder):
            # Check that the work folder of the executor was initialized
            assert os.path.exists(cluster_folder)


@skip_if_no_backend
def test_executor_map():
    with TemporaryDirectory(dir=BASES_SHARED_FOLDER) as test_folder:
        cluster_folder = os.path.join(test_folder, 'clusterlib')
        with ClusterExecutor(folder=cluster_folder, **EXECUTOR_PARAMS) as e:
            results = e.map(_increment, range(10))
            assert_equal(list(results), list(range(1, 11)))

            # When the callable raises an exception, only the first exception
            # is raised upon consumption of the first item of the results
            # generator
            results = e.map(_raise_exc, [ValueError(), AttributeError()])
            assert_raises(ValueError, list, results)

            results = e.map(_raise_exc, [AttributeError(), ValueError()])
            assert_raises(AttributeError, list, results)

            # When the callable raises an exception, only the first exception
            # is raised
            results = e.map(_raise_exc, [CustomException(), ValueError()])
            assert_raises(CustomException, list, results)


@skip_if_no_backend
def test_executor_submit():
    with TemporaryDirectory(dir=BASES_SHARED_FOLDER) as test_folder:
        cluster_folder = os.path.join(test_folder, 'clusterlib')
        with ClusterExecutor(folder=cluster_folder, **EXECUTOR_PARAMS) as e:
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
def test_executor_job_duplication():
    with TemporaryDirectory(dir=BASES_SHARED_FOLDER) as test_folder:
        cluster_folder = os.path.join(test_folder, 'clusterlib')
        with ClusterExecutor(folder=cluster_folder, **EXECUTOR_PARAMS) as e:
            f1 = e.submit(sleep, 100)
            f2 = e.submit(sleep, 100)

            # the 2 jobs are mapped to the same folder because they share the
            # same arguments
            assert_equal(f1.job_name, f2.job_name)
            assert_false(f1.done())
            assert_false(f2.done())
            assert_false(f1.cancelled())
            assert_false(f2.cancelled())
            if f1.running():
                assert_true(f2.running())
            sleep(0.1)
            if f2.running():
                assert_true(f1.running())

            # Cancelling one job will automatically cancel the other
            assert_true(f2.cancel(interrupt_running=True))
            assert_true(f1.cancelled())
            assert_true(f2.cancelled())


def _check_self_is_running():
    job_folder = os.environ['CLUSTERLIB_JOB_FOLDER']
    return AtomicMarker(job_folder, 'running').isset()


@skip_if_no_backend
def test_running_marker_from_job():
    with TemporaryDirectory(dir=BASES_SHARED_FOLDER) as test_folder:
        cluster_folder = os.path.join(test_folder, 'clusterlib')
        with ClusterExecutor(folder=cluster_folder, **EXECUTOR_PARAMS) as e:
            f = e.submit(_check_self_is_running)
            assert_true(f.result())

            # Once the job has completed, the running marker is no longer set
            assert_false(AtomicMarker(f._job_folder, 'running').isset())


def _send_signal_to_self(signum):
    os.kill(os.getpid(), signum)


@skip_if_no_backend
def test_executor_cancel_by_signal():
    with TemporaryDirectory(dir=BASES_SHARED_FOLDER) as test_folder:
        cluster_folder = os.path.join(test_folder, 'clusterlib')
        with ClusterExecutor(folder=cluster_folder, **EXECUTOR_PARAMS) as e:
            futures = [e.submit(_send_signal_to_self, s)
                       for s in CANCELLATION_SIGNALS]
            for f in futures:
                # Those jobs can never complete: they will automatically
                # cancel them selves before being able
                assert_false(f.done())

                # The jobs have only a very short window to be running
                assert_in(f.running(), [True, False])

                # Some of them might already be cancelled at this point
                assert_in(f.cancelled(), [True, False])

            # Block to the results, instead this should raise a cancellation
            # error
            for f in futures:
                assert_raises(ClusterCancelledError, f.result)
                assert_raises(ClusterCancelledError, f.exception)

            for f in futures:
                # All jobs should be in state 'cancelled' at this point
                assert_true(f.cancelled())

                # Cancelled jobs are not done nor running
                assert_false(f.done())
                assert_false(f.running())


@skip_if_no_backend
def test_executor_cancel():
    with TemporaryDirectory(dir=BASES_SHARED_FOLDER) as test_folder:
        cluster_folder = os.path.join(test_folder, 'clusterlib')
        with ClusterExecutor(folder=cluster_folder, **EXECUTOR_PARAMS) as e:
            f1 = e.submit(sleep, 100)
            f2 = e.submit(sleep, 200)

            for f in [f1, f2]:
                assert_false(f.done())
                assert_in(f.running(), [True, False])
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

            # The first job is still running (or pending in the queue)
            assert_false(f1.done())
            assert_in(f.running(), [True, False])
            assert_false(f1.cancelled())

            # Cancel it as well
            assert_true(f1.cancel(interrupt_running=True))
            assert_false(f1.done())
            assert_true(f1.cancelled())
            assert_raises(ClusterCancelledError, f1.result)
            assert_raises(ClusterCancelledError, f1.exception)
