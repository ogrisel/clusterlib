from clusterlib.futures import ClusterExecutor
from math import log
import logging

# Change the logging level to DEBUG to introspect what's happening under the
# hood
logging.basicConfig(format='%(levelname)s: %(message)s', level=logging.INFO)


with ClusterExecutor(poll_interval=1) as e:
    # Parallel map
    values = list(range(1, 11))
    print("About to call 'log' on values %r in parallel:" % values)
    for value, result in zip(values, e.map(log, values)):
        print("Result for input %d: %f" % (value, result))

    # Finer control, future based API
    print("Calling log twice in parallel without blocking")
    f1 = e.submit(log, 1)
    f2 = e.submit(log, 100, 10)

    # Duplicate calls should be detected and share the same job_folder
    # and worker
    f3 = e.submit(log, 100, 10)

    print("Is f1 done yet? %s" % f1.done())
    print("Is f2 done yet? %s" % f2.done())
    print("Is f3 done yet? %s" % f3.done())

    print("Waiting to collect the results...")
    print("Result of f1: %s" % f1.result())
    print("Result of f2: %s" % f2.result())
    print("Result of f3: %s" % f3.result())
