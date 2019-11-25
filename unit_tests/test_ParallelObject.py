import unittest
import time
import sys
import logging


logging.basicConfig(level='INFO')
LOGGER = logging.getLogger(name='parallelobject')

sys.path.append('/home/abykov/Projects/scylladb-project/scylla-cluster-tests')

from sdcm.utils.common import ParallelObject


rand_timeout = [15, 5, 30]


def my_func(timeout):
    LOGGER.info('start')
    time.sleep(timeout)
    LOGGER.info('finished')
    return (timeout, 'test')


t = ParallelObject(rand_timeout, timeout=6, num_workers=3, disable_logging=False)

results = t.run(my_func, ignore_exceptions=True)

LOGGER.info(results)

for r in results:
    if r.exc:
        LOGGER.info("%s", r.exc.__class__)
