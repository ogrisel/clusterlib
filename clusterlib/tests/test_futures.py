import os

from nose.tools import assert_equal

from clusterlib.futures import ClusterExecutor
from clusterlib.testing import TemporaryDirectory, skip_if_no_backend


def _increment(a, step=1):
    """Dummy test function to call in parallel"""
    return a + step


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
