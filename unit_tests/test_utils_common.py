import logging
import unittest

from sdcm.utils.common import tag_ami


logging.basicConfig(level=logging.DEBUG)


class TestUtils(unittest.TestCase):
    def test_tag_ami_01(self):  # pylint: disable=no-self-use
        tag_ami(ami_id='ami-0876bfff890e17a06',
                tags_dict={'JOB_longevity-multi-keyspaces-60h': 'PASSED'}, region_name='eu-west-1')
