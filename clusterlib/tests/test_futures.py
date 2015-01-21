from tempfile import mkdtemp
from shutil import rmtree
import os

from clusterlib.futures import ClusterExecutor


def test_executor_folder():
    try:
        test_folder = mkdtemp()
        cluster_folder = os.path.join(test_folder, 'clusterlib')
        with ClusterExecutor(folder=cluster_folder):
            assert os.path.exists(cluster_folder)
    finally:
        rmtree(test_folder)
