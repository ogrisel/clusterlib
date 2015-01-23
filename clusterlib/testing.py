"""Utilities for testing

Note: this module has a dependency on the nose package.

"""
# Authors: Olivier Grisel
#
# License: BSD 3 clause
import shutil
import weakref
import warnings
from tempfile import mkdtemp

from nose import SkipTest
from nose.tools import with_setup

from clusterlib.scheduler import _get_backend


class TemporaryDirectory(object):
    """Create and return a temporary directory.  This has the same
    behavior as mkdtemp but can be used as a context manager.  For
    example:

        with TemporaryDirectory() as tmpdir:
            ...

    Upon exiting the context, the directory and everything contained
    in it are removed.

    Note: this class backported from the Python 3.4 stdlib for backward
    compat with Python 2.

    """

    # Handle mkdtemp raising an exception
    name = None
    _finalizer = None
    _closed = False

    def __init__(self, suffix="", prefix="tmp", dir=None):
        self.name = mkdtemp(suffix, prefix, dir)
        self._finalizer = weakref.finalize(
            self, self._cleanup, self.name,
            warn_message="Implicitly cleaning up {!r}".format(self))

    @classmethod
    def _cleanup(cls, name, warn_message=None):
        shutil.rmtree(name)
        if warn_message is not None:
            warnings.warn(warn_message, ResourceWarning)

    def __repr__(self):
        return "<{} {!r}>".format(self.__class__.__name__, self.name)

    def __enter__(self):
        return self.name

    def __exit__(self, exc, value, tb):
        self.cleanup()

    def cleanup(self):
        if self._finalizer is not None:
            self._finalizer.detach()
        if self.name is not None and not self._closed:
            shutil.rmtree(self.name)
            self._closed = True


def _skip_if_no_backend():
    try:
        _get_backend(backend='auto')
    except RuntimeError:
        raise SkipTest('A scheduler backend is required for this test.')


skip_if_no_backend = with_setup(_skip_if_no_backend)
