# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License as published by
# the Free Software Foundation; either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
#
# See LICENSE for more details.
#
# Copyright (c) 2022 ScyllaDB
from dataclasses import dataclass

import pytest

from sdcm.utils.data_validator import LongevityDataValidator
from sdcm import sct_config


@dataclass
class MockLongevityTest:
    params: sct_config.SCTConfiguration


@pytest.mark.sct_config(files='unit_tests/test_data/test_data_validator/lwt-basic-3h.yaml')
def test_view_names_for_updated_data(params):
    data_validator = LongevityDataValidator(longevity_self_object=MockLongevityTest(params=params),
                                            user_profile_name='c-s_lwt',
                                            base_table_partition_keys=['domain', 'published_date'])
    data_validator._validate_updated_per_view = [True, True]
    views_list = data_validator.list_of_view_names_for_update_test()
    assert views_list == [('blogposts_update_one_column_lwt_indicator',
                           'blogposts_update_one_column_lwt_indicator_after_update',
                           'blogposts_update_one_column_lwt_indicator_expect', True),
                          ('blogposts_update_2_columns_lwt_indicator',
                           'blogposts_update_2_columns_lwt_indicator_after_update',
                           'blogposts_update_2_columns_lwt_indicator_expect', True)]


@pytest.mark.sct_config(files='unit_tests/test_data/test_data_validator/no-validation-views-lwt-basic-3h.yaml')
def test_view_names_for_updated_data_not_found(params):

    data_validator = LongevityDataValidator(longevity_self_object=MockLongevityTest(params=params),
                                            user_profile_name='c-s_lwt',
                                            base_table_partition_keys=['domain', 'published_date'])
    data_validator._validate_updated_per_view = [True, True]
    views_list = data_validator.list_of_view_names_for_update_test()
    assert views_list == []
