"""Implementation of the concurrent.futures API for a cluster

See:

https://docs.python.org/3/library/concurrent.futures.html

"""
import os
import os.path as op
import time
import sys
import logging
import socket
try:
    import cPickle as pickle  # Python 2 compat
except ImportError:
    import pickle

from clusterlib.scheduler import submit

logger = logging.getLogger('clusterlib')

try:
    # Try to use joblib for efficient pickling of numpy arrays with
    # support for memory mapping
    import joblib
except ImportError:
    logger.info("joblib is not available: falling back to default pickling")
    joblib = None

CANCELLED = 'cancelled'
RUNNING = 'running'
FINISHED = 'finished'
PENDING = 'pending'
DEFAULT_POLL_INTERVAL = 10  # in seconds

try:
    from concurrent.futures import TimeoutError, CancelledError

    class ClusterTimeoutError(TimeoutError):
        pass

    class ClusterCancelledError(CancelledError):
        pass

except ImportError:
    # define our own exception classes to keep the dependency
    # on the "futures" package optional under Python 2.
    # concurrent.futures is shipped by default in the Python 3
    # standard library
    class ClusterTimeoutError(Exception):
        pass

    class ClusterCancelledError(Exception):
        pass


def safe_makedirs(folder):
    """Protect makedirs for concurrent."""
    try:
        os.makedirs(folder)
    except OSError:
        if not op.exists(folder):
            raise
        # else: folder already exists: ignore


def _dump(obj, filename):
    if joblib is not None:
        joblib.dump(obj, filename)
    else:
        with open(filename, 'wb') as f:
            pickle.dump(obj, f)


def _load(filename, mmap_mode=None):
    if joblib is not None:
        return joblib.load(filename, mmap_mode=mmap_mode)
    else:
        with open(filename, 'rb') as f:
            return pickle.load(f)


class ClusterExecutor(object):

    def __init__(self, folder='clusterlib', backend='auto', min_memory=4000,
                 job_max_time='24:00:00',
                 poll_interval=DEFAULT_POLL_INTERVAL):
        self.folder = os.path.abspath(folder)
        self.backend = backend
        self.min_memory = min_memory
        self.job_max_time = job_max_time
        self.poll_interval = poll_interval
        safe_makedirs(folder)

    def __enter__(self):
        return self

    def _get_finished_future(self, job_folder, job_name, fn, args, kwargs):
        output_filename = op.join(job_folder, 'output.pkl')
        if op.exists(output_filename):
            # The same job has already been submitted in the past and
            # completed successfully: collect the output and return directly.
            logger.debug('Reloading existing job output: %s', output_filename)
            try:
                result = _load(output_filename)
                return ClusterFuture(job_name, self, fn, args, kwargs,
                                     status=FINISHED, result=result)
            except EOFError:
                logger.debug('Invalid output file: %s, resubmitting',
                             output_filename)
                pass

        exception_filename = op.join(job_folder, 'exception.pkl')
        if op.exists(exception_filename):
            # The same job has already been submitted in the past and
            # failed: collect the exception and return directly.
            logger.debug('Reloading existing job exception: %s',
                         exception_filename)
            try:
                exception = _load(exception_filename)
                return ClusterFuture(job_name, self, fn, args, kwargs,
                                     status=FINISHED, exception=exception)
            except EOFError:
                logger.debug('Invalid exception file: %s, resubmitting',
                             exception_filename)
                pass

    def submit(self, fn, *args, **kwargs):
        job_hash = joblib.hash((fn, args, kwargs))
        job_name = fn.__name__ + '-' + job_hash
        job_folder = op.abspath(op.join(self.folder, job_name))
        safe_makedirs(job_folder)

        # TODO: find a better API for _get_finished_future
        finished_future = self._get_finished_future(job_folder, job_name,
                                                    fn, args, kwargs)
        if finished_future is not None:
            return finished_future

        # Dump the input
        _dump(fn, op.join(job_folder, 'callable.pkl'))
        _dump((args, kwargs), op.join(job_folder, 'input.pkl'))

        cancelled_filename = op.join(job_folder, 'cancelled')
        if op.exists(cancelled_filename):
            # If job was cancelled in the past, un-cancel it before
            # resubmitting it
            os.unlink(cancelled_filename)
        self._dispatch_job(job_name, job_folder)
        return ClusterFuture(job_name, self, fn, args, kwargs, status=PENDING)

    def _dispatch_job(self, job_name, job_folder):
        cmd = "%s -m clusterlib.futures %s" % (sys.executable, job_folder)
        # TODO: pass additional cluster options here
        submit_cmd = submit(cmd, job_name=job_name, time=self.job_max_time,
                            memory=self.min_memory, backend=self.backend)
        logger.debug(submit_cmd)
        code = os.system(submit_cmd)
        if code != 0:
            raise RuntimeError('Command "%s" returned code %s'
                               % (submit_cmd, code))

    def map(self, fn, *iterables, **kwargs):
        timeout = kwargs.get('timeout')  # Python 2 compat
        if timeout is not None:
            end_time = timeout + time.time()

        futures = [self.submit(fn, *args) for args in zip(*iterables)]
        try:
            for future in futures:
                if timeout is None:
                    yield future.result()
                else:
                    yield future.result(end_time - time.time())
        finally:
            for future in futures:
                future.cancel()

    def __exit__(self, e_type, e_value, e_traceback):
        # TODO: put any cleanup logic necessary here
        pass

    def _update_job_status(self, future):
        job_folder = op.join(self.folder, future.job_name)
        f = self._get_finished_future(
            job_folder, future.job_name, future._callable, future._input_args,
            future._input_kwargs)
        if f is not None:
            # TODO: there must be a better way to do it
            future._status = f._status
            future._result = f._result
            future._exception = f._exception
        # TODO: detect cancelled jobs

    def _cancel_job(self, future):
        pass


class ClusterFuture(object):

    def __init__(self, job_name, executor, fn, args, kwargs, status=PENDING,
                 result=None, exception=None):
        self.job_name = job_name
        self._callable = fn
        self._input_args = args
        self._input_kwargs = kwargs
        self._executor = executor
        self._result = result
        self._exception = exception
        self._status = status

    def cancel(self, interrupt_running=False):
        """Remove the job from the queue.

        Return True if the job was cancelled.
        Return False if the job has already completed.

        If the job is already running and interrupt_running is True, a kill
        signal will be sent to the running process by the cluster scheduler.

        """
        return self._executor._cancel_job(self)

    def cancelled(self):
        """Return True if the call was successfully cancelled."""
        self._executor._update_job_status(self)
        return self._status == CANCELLED

    def running(self, timeout=None):
        """Return True if the call is currently being executed"""
        self._executor._update_job_status(self)
        return self._status == RUNNING

    def done(self):
        self._executor._update_job_status(self)
        return self._status == FINISHED

    def result(self, timeout=None):
        start_tic = time.time()
        while True:
            self._executor._update_job_status(self)
            if self._status == CANCELLED:
                # TODO: add informative exception message
                raise ClusterCancelledError()
            if self._status == FINISHED:
                if self._exception is not None:
                    # TODO: check that this is the behavior as for other
                    # implementations of futures
                    raise self._exception
                return self._result

            next_tic = time.time() + self._executor.poll_interval
            if (timeout is not None and (next_tic - start_tic) > timeout):
                # TODO: add informative exception message
                raise ClusterTimeoutError()

            # Wait before refreshing the status of the job or timeout
            time.sleep(self._executor.poll_interval)

    def exception(self, timeout=None):
        start_tic = time.time()
        while True:
            self._executor._update_job_status(self)
            if self._status == CANCELLED:
                # TODO: add informative exception message
                raise ClusterCancelledError()
            if self._status == FINISHED:
                return self._exception

            next_tic = time.time() + self._executor.poll_interval
            if (timeout is not None and (next_tic - start_tic) > timeout):
                # TODO: add informative exception message
                raise ClusterTimeoutError()

            # Wait before refreshing the status of the job or timeout
            time.sleep(self._executor.poll_interval)


def execute_job(job_folder):
    """Function to be executed by the worker node"""
    try:
        func = _load(op.join(job_folder, 'callable.pkl'))
        args, kwargs = _load(op.join(job_folder, 'input.pkl'),
                                   mmap_mode='r')
        results = func(*args, **kwargs)
        _dump(results, op.join(job_folder, 'output.pkl'))
    except InterruptedError:
        logger.debug("Job in %s was interrupted by host", job_folder)
        with open(op.join(job_folder, 'cancelled'), 'wb') as f:
            # TODO: log more info in a json formatted file instead?
            f.write(socket.gethostname())
    except Exception as e:
        logger.debug("Job in %s raised %s", job_folder)
        _dump(e, op.join(job_folder, 'exception.pkl'))


if __name__ == '__main__':
    # This module is called directly by the cluster queue
    if len(sys.argv) < 2:
        print('Pass the job folder as argument to execute its content')
    else:
        execute_job(sys.argv[1])
