"""Implementation of the concurrent.futures API for a cluster

See:

https://docs.python.org/3/library/concurrent.futures.html

"""
# Authors: Olivier Grisel
#
# License: BSD 3 clause
from __future__ import print_function
from getpass import getuser
import os
import os.path as op
from time import time, sleep
import sys
import logging
import joblib
import signal
import errno

from clusterlib.scheduler import submit, queued_or_running_jobs

logger = logging.getLogger('clusterlib')
TRACE = logging.DEBUG - 5  # Even more verbose than DEBUG


CANCELLED = 'cancelled'
RUNNING = 'running'
FINISHED = 'finished'
PENDING = 'pending'
DEFAULT_POLL_INTERVAL = 10  # in seconds
CANCELLATION_SIGNALS = [
    signal.SIGTERM,
    signal.SIGABRT,
    signal.SIGQUIT,
]
MAX_FS_CACHE_DELAY = 100


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
    joblib.dump(obj, filename)


def _load(filename, mmap_mode=None):
    return joblib.load(filename, mmap_mode=mmap_mode)


class AtomicMarkerError(Exception):
    pass


class AtomicMarker(object):
    """Named marker for coordination between concurrent processes

    clusterlib leverages a shared filesystem to exchange data between
    the main coordination script and worker processes.

    Such filesystem have typically very few atomic operations. This
    implementation leverages the atomic the mkdir system call many
    UNIX filesystems (NFS included) to implement filesystem based
    markers to enable coordination between concurrent cluster processes.

    """

    def __init__(self, job_folder, marker_id):
        self.marker_path = op.join(job_folder, marker_id)

    def __enter__(self):
        self.set()
        return self

    def __exit__(self, e_type, e_value, e_traceback):
        self.unset()

    def isset(self):
        """Check the presence of the marker

        This call is should not be fooled by the cache of parallel filesystem
        such as NFS.
        """
        try:
            # We cannot just trust os.path.isdir as the NFS client cache might
            # hide newly created markers. Hence we do a dummy call to open a
            # file with write access on the same path to force NFS cache
            # invalidation for that path. If the marker directory exists, we
            # get an error message with a specific errno. Otherwise we
            # unlink our newly created file.
            open(self.marker_path, 'wb')
            try:
                os.unlink(self.marker_path)
            except (OSError, IOError) as e:
                if getattr(e, 'errno', None) == errno.ENOENT:
                    # A concurrent call to isset might have deleted
                    # our file: this is not a problem.
                    return False
                elif getattr(e, 'errno', None) == errno.EISDIR:
                    # A concurrent call to set has created the marker
                    # directory
                    return True
                else:
                    raise
            # We were able to create a temporary file at the marker meaning
            # that there is no directory with that same path
            return False
        except (OSError, IOError) as e:
            if getattr(e, 'errno', None) == errno.EISDIR:
                # This path points to a directory: the marker is set.
                return True
            elif getattr(e, 'errno', None) in (errno.ENOENT, errno.EEXIST):
                # Another process might be calling isset concurrently
                # which can trigger those errors due to the lack of atomicity
                # of open without O_EXCL: we cannot use O_EXCL because it is
                # not properly supported in NFSv2 and v3.
                # In any case this path is not a directory: returning False.
                return False
            raise

    def set(self, n_retries=10):
        """Enable the marker

        Return True if we are responsible to setting the marker.
        Return False if the marker was previously set or set concurrently
        by another process.

        To ensure correctness under highly concurrent access (notably
        if concurrent process actively probe the presence of the proble
        with calls to `open`, setting the marker can take a while and
        sometimes fail with AtomicMarkerError.
        """
        try:
            os.mkdir(self.marker_path)
            return True
        except (OSError, IOError) as e:
            if getattr(e, 'errno', None) == errno.EEXIST:
                # This can be cause by a the transient file generated by
                # a concurrent call to isset or by an existing directory
                # marker: we cannot trust isdir, so let us call isset
                # ourselves:
                if self.isset():
                    # A concurrent process has set the marker, return False
                    # to indicate that we are not the creator.
                    return False
                else:
                    # Wait a bit and retry
                    if n_retries <= 0:
                        raise AtomicMarkerError(
                            'Failed to set marker at "%s"'
                            % self.marker_path)
                    sleep(0.1)
                    return self.set(n_retries=n_retries - 1)
            else:
                raise  # this is not expected

    def unset(self):
        """Ensure that the marker has been removed"""
        try:
            os.rmdir(self.marker_path)
            return True
        except (OSError, IOError) as e:
            if getattr(e, 'errno', None) == errno.ENOENT:
                # marker has already been unset: ignore
                return False
            elif getattr(e, 'errno', None) == errno.ENOTDIR:
                # a concurrent call to isset might have created
                # a short-lived file: let us ignore it.
                return False
            else:
                raise  # this is not expected


class ClusterExecutor(object):
    """Context manager to schedule Python jobs on a cluster"""

    def __init__(self, folder='clusterlib', backend='auto', min_memory=4000,
                 job_max_time='24:00:00',
                 poll_interval=DEFAULT_POLL_INTERVAL):
        self.folder = os.path.abspath(folder)
        self.backend = backend
        self.min_memory = min_memory
        self.job_max_time = job_max_time
        self.poll_interval = poll_interval
        self.user = getuser()
        safe_makedirs(folder)

    def __enter__(self):
        return self

    def __exit__(self, e_type, e_value, e_traceback):
        # TODO: put any cleanup logic necessary here
        pass

    def _get_finished_future(self, job_folder, job_name, fn, args, kwargs,
                             raise_on_invalid=True):
        finished_marker = AtomicMarker(job_folder, 'finished')
        if not finished_marker.isset():
            return None

        # Try to invalid the NFS negative-entry cache under Linux by calling
        # chown on the parent folder without changing the id of the owner.
        # Note: this strategy is not garanteed to work on all File Systems
        # but has proved to help with NFSv3 clients under Linux.
        s = os.stat(job_folder)
        os.chown(job_folder, s.st_uid, s.st_gid)

        output_filename = op.join(job_folder, 'output.pkl')
        if op.exists(output_filename):
            # The same job has already been submitted in the past and
            # completed successfully: collect the output and return directly.
            logger.debug('Loading job output: %s', output_filename)
            try:
                result = _load(output_filename)
                return ClusterFuture(job_name, job_folder, self, fn, args,
                                     kwargs, status=FINISHED, result=result)
            except EOFError:
                if raise_on_invalid:
                    raise
                logger.warn('Invalid output file: %s', output_filename)
                return None

        exception_filename = op.join(job_folder, 'exception.pkl')
        if op.exists(exception_filename):
            # The same job has already been submitted in the past and
            # failed: collect the exception and return directly.
            logger.debug('Loading job exception: %s', exception_filename)
            try:
                exception = _load(exception_filename)
                return ClusterFuture(job_name, job_folder, self, fn, args,
                                     kwargs, status=FINISHED,
                                     exception=exception)
            except EOFError:
                if raise_on_invalid:
                    raise
                logger.warn('Invalid exception file: %s', exception_filename)
                return None

    def submit(self, fn, *args, **kwargs):
        job_hash = joblib.hash((fn, args, kwargs))
        job_name = fn.__name__ + '-' + job_hash
        job_folder = op.abspath(op.join(self.folder, job_name))
        safe_makedirs(job_folder)

        # TODO: find a better API for _get_finished_future
        finished_future = self._get_finished_future(job_folder, job_name,
                                                    fn, args, kwargs,
                                                    raise_on_invalid=False)
        if finished_future is not None:
            # The same job has already completed in the past, let's reuse it.
            return finished_future

        # Dump the input
        _dump(fn, op.join(job_folder, 'callable.pkl'))
        _dump((args, kwargs), op.join(job_folder, 'input.pkl'))

        # If job was cancelled in the past, remove the cancellation marker
        # before resubmiting it
        AtomicMarker(job_folder, 'cancelled').unset()

        # Perform the actual dispath
        self._dispatch_job(job_name, job_folder)
        return ClusterFuture(job_name, job_folder, self, fn, args, kwargs,
                             status=PENDING)

    def _dispatch_job(self, job_name, job_folder):
        cmd = "%s -m clusterlib.futures %s" % (sys.executable, job_folder)
        # TODO: pass additional cluster options here
        submit_cmd = submit(cmd, job_name=job_name, time=self.job_max_time,
                            memory=self.min_memory, backend=self.backend)
        # logger.log(TRACE, submit_cmd)
        logger.debug(submit_cmd)
        code = os.system(submit_cmd)
        if code != 0:
            raise RuntimeError('Command "%s" returned code %s'
                               % (submit_cmd, code))

    def map(self, fn, *iterables, **kwargs):
        timeout = kwargs.get('timeout')  # Python 2 compat
        if timeout is not None:
            end_time = timeout + time()

        futures = [self.submit(fn, *args) for args in zip(*iterables)]
        try:
            for future in futures:
                if timeout is None:
                    yield future.result()
                else:
                    yield future.result(timeout=end_time - time())
        finally:
            for future in futures:
                future.cancel()

    def _update_job_status(self, future):
        # Check whether the job is active according to the scheduler
        is_queued_or_running = future.job_name in queued_or_running_jobs(
            user=self.user)
        if is_queued_or_running:
            logger.debug('job %s is queued or running according to scheduler',
                         future.job_name)
        else:
            logger.debug('job %s is no longer referenced in the scheduler',
                         future.job_name)

        # Introspect the markers in the job folder
        job_folder = op.join(self.folder, future.job_name)

        cancel_marker = AtomicMarker(job_folder, 'cancelled')
        if cancel_marker.isset():
            logger.debug('job %s has the "cancelled" marker', future.job_name)
            future._status = CANCELLED
            return

        finished_marker = AtomicMarker(job_folder, 'finished')
        if finished_marker.isset():
            logger.debug('job %s has the "finished" marker enabled',
                         future.job_name)
            if self._update_finished_job_status(future):
                logger.debug('job %s is in state "finished"', future.job_name)
                # The job has finished (yield either a result or an exception)
                return
            else:
                start_time = future._wait_for_results_start_time
                if start_time is None:
                    future._wait_for_results_start_time = start_time = time()
                elif (time() - start_time) > MAX_FS_CACHE_DELAY:
                    future._exception = OSError(
                        'Time exceeded while reading results from parallel'
                        ' filesystem for job %s' % future.job_name)
                    future._status = FINISHED
                    return

                logger.debug('job %s waiting the results in state "running"',
                             future.job_name)
                future._status = RUNNING
                return

        running_marker = AtomicMarker(job_folder, 'running')
        if running_marker.isset():
            logger.debug('job %s has the "running" marker', future.job_name)
            if is_queued_or_running:
                logger.debug('job %s is in state "running"', future.job_name)
                # Everything looks fine
                future._status = RUNNING
                return
            else:
                # The jobs must have silently crashed or be killed without
                # a receiving a SIGTERM signal first.
                logger.debug('marking job %s as crashed with an exception',
                             future.job_name)
                e = RuntimeError('%s terminated silently while running'
                                 % future.job_name)
                _dump(e, op.join(job_folder, 'exception.pkl'))
                # Mark finished and cleanup the running marker:
                finished_marker.set()
                running_marker.unset()

                # Let the next iteration collect the finished results as usual
                future._status = RUNNING
                return

        # At this point, any unfinished (and not running) job is either pending
        # or silently cancelled by the scheduler before the start of the
        # execution (e.g. via a manual call to qdel)
        if is_queued_or_running:
            logger.debug('job %s is in state "pending"', future.job_name)
            future._status = PENDING
        else:
            logger.debug('job %s is in state "cancelled"', future.job_name)
            cancel_marker.set()
            future._status = CANCELLED

    def _update_finished_job_status(self, future, n_retries=3):
        for i in range(n_retries):
            try:
                f = self._get_finished_future(
                    future._job_folder, future.job_name, future._callable,
                    future._input_args, future._input_kwargs)
                if f is not None:
                    future._status = f._status
                    future._result = f._result
                    future._exception = f._exception
                    return True
                else:
                    return False
            except EOFError:
                # This can happen if the worker is concurrently serializing
                # the output or the exception.
                if i < n_retries:
                    pause_duration = 5
                    logger.debug('Sleeping %d seconds before next retry',
                                 pause_duration)
                    sleep(pause_duration)
                else:
                    # Retry credits exhausted: re-raise the exception to the
                    # caller
                    raise


class ClusterFuture(object):

    def __init__(self, job_name, job_folder, executor, fn, args, kwargs,
                 status=PENDING, result=None, exception=None):
        self.job_name = job_name
        self._job_folder = job_folder
        self._callable = fn
        self._input_args = args
        self._input_kwargs = kwargs
        self._executor = executor
        self._result = result
        self._exception = exception
        self._status = status
        self._wait_for_results_start_time = None

    def cancel(self, interrupt_running=False):
        """Remove the job from the queue.

        Return True if the job was cancelled.
        Return False if the job has already completed.

        If the job is already running and interrupt_running is True, a kill
        signal will be sent to the running process by the cluster scheduler.

        """
        self._executor._update_job_status(self)
        if self._status == FINISHED:
            return False

        if self._status == RUNNING and not interrupt_running:
            return False

        if self._status == CANCELLED:
            return True

        # We use a directory as job cancelation marker as creating and
        # deleting a directory is a cheap and atomic operations under
        # Unix / NFS
        AtomicMarker(self._job_folder, 'cancelled').set()
        self._status = CANCELLED

        # TODO: actually call qdel to cleanup free the cluster resources
        # and avoid or interrupt the execution of cancelled jobs
        return True

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
        start_tic = time()
        while True:
            self._executor._update_job_status(self)
            if self._status == CANCELLED:
                raise ClusterCancelledError('Job %s was cancelled'
                                            % self.job_name)
            if self._status == FINISHED:
                if self._exception is not None:
                    # TODO: check that this is the behavior as for other
                    # implementations of futures
                    raise self._exception
                return self._result

            if (timeout is not None and (time() - start_tic) > timeout):
                raise ClusterTimeoutError(
                    'Timeout getting result for job %s in state %s after'
                    ' more than %0.3fs'
                    % (self.job_name, self._status, timeout))

            # Wait before refreshing the status of the job or timeout
            interval = self._executor.poll_interval
            logger.debug('Waiting %0.3fs for the result of %s in state %s',
                         interval, self.job_name, self._status)
            sleep(interval)

    def exception(self, timeout=None):
        start_tic = time()
        while True:
            self._executor._update_job_status(self)
            if self._status == CANCELLED:
                raise ClusterCancelledError('Job %s was cancelled'
                                            % self.job_name)
            if self._status == FINISHED:
                return self._exception

            if (timeout is not None and (time() - start_tic) > timeout):
                raise ClusterTimeoutError(
                    'Timeout getting exception for job %s in state %s after'
                    ' more than %0.3fs'
                    % (self.job_name, self._status, timeout))

            # Wait before refreshing the status of the job or timeout
            interval = self._executor.poll_interval
            logger.debug('Waiting %0.3fs for the exception of %s in state %s',
                         interval, self.job_name, self._status)
            sleep(interval)


#
# The followin functions are meant to be executed on the scheduler worker
# processes: log debug and error message on stderr to leverage the stderr
# captures provided by the scheduler systems.
#

def _job_log(message):
    job_name = os.environ.get('JOB_NAME', os.environ.get('SLURM_JOB_NAME'))
    job_id = os.environ.get('JOB_ID', os.environ.get('SLURM_JOB_ID'))
    if job_name is not None and job_id is not None:
        message = ("[%s|%s] " % (job_name, job_id)) + message
    print(message, file=sys.stderr)
    sys.stderr.flush()


def _make_cancellation_handler(job_folder, running_marker):
    def handler(signum, frame):
        _job_log("Job interrupted by signal %d" % signum)
        AtomicMarker(job_folder, 'cancelled').set()
        running_marker.unset()
        sys.exit(0)
    return handler


def execute_job(job_folder):
    """Function to be executed by the worker node

    The main purpose of this wrapper is to:
      - load the callable and its input arguments pickled in the job folder
      - actually call the callable with those arguments
      - collect the result(s) of the call and pickle them in the job folder
      - collect any raised exception and pickle it in the job folder

    This wrapper also takes care of updaing the 'running' and 'cancelled'
    state markers via symbolic links also stored in the job folder.

    """
    finished_marker = AtomicMarker(job_folder, 'finished')
    running_marker = AtomicMarker(job_folder, 'running')
    cancel_marker = AtomicMarker(job_folder, 'cancelled')
    if finished_marker.isset():
        _job_log("The same job has already completed in the past: skipping")
        return

    # Put the running marker into this job folder. This marke also serves as
    # a protection against concurrent execution of the same job twice.
    try:
        owner_of_running_marker = running_marker.set()
        if not owner_of_running_marker:
            # The running_marker was already set by some past or concurrent
            # process: do not duplicate work to avoid corrupting the output
            # with concurrent writes to the same file(s).
            _job_log("The same job is concurrently running: skipping")
            return

        # Make it possible for the callable to introspect its clusterlib
        # job_folder by passing it as an environment variable. This
        # is mostly useful for testing and debuging
        os.environ['CLUSTERLIB_JOB_FOLDER'] = job_folder

        # Register a signal handler to capture job cancellation signals such
        # as a triggered by SLURM's scancel command.
        handler = _make_cancellation_handler(job_folder, running_marker)
        for signum in CANCELLATION_SIGNALS:
            _job_log("Registering handler for signal %d" % signum)
            signal.signal(signum, handler)

        try:
            _job_log("Loading callable and arguments")
            func = _load(op.join(job_folder, 'callable.pkl'))
            args, kwargs = _load(op.join(job_folder, 'input.pkl'),
                                 mmap_mode='r')

            if cancel_marker.isset():
                _job_log("The job was cancelled concurrently")
                return

            _job_log("Executing callable: %r" % func)
            results = func(*args, **kwargs)
            _job_log("Writing results")
            _dump(results, op.join(job_folder, 'output.pkl'))
        except Exception as e:
            _job_log("Exception raised: %s" % e)
            _dump(e, op.join(job_folder, 'exception.pkl'))
        if finished_marker.set():
            _job_log("Successfully set the 'finished' marker")
        else:
            _job_log("ERROR: failed to set the 'finished' marker")
    finally:
        if owner_of_running_marker:
            if running_marker.unset():
                _job_log("Successfully unset the 'running' marker")
            else:
                _job_log("ERROR: failed to unset the 'running' marker")


if __name__ == '__main__':
    # This module is called directly by the cluster queue
    if len(sys.argv) < 2:
        print('Pass the job folder as argument to execute its content')
    else:
        execute_job(sys.argv[1])
