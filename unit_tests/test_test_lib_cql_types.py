import unittest
from test_lib.cql_types import CQLTypeBuilder, NOT_EMBEDDABLE_COLUMN_TYPES, COLLECTION_COLUMN_TYPES, ALL_COLUMN_TYPES


class CQLColumnTypeTest(unittest.TestCase):

    def test_get_random(self):

        already_used = {}
        EMBEDDABLE_NON_COLLECTION_COLUMN_TYPES = \
            [e for e in ALL_COLUMN_TYPES if e not in NOT_EMBEDDABLE_COLUMN_TYPES and e not in COLLECTION_COLUMN_TYPES]
        NON_COLLECTION_TYPES = [e for e in ALL_COLUMN_TYPES if e not in COLLECTION_COLUMN_TYPES]
        for e in NON_COLLECTION_TYPES:
            random_type = CQLTypeBuilder.get_random(already_used, allow_levels=0)
            random_type.remember_variant(already_used)
        assert set(already_used.keys()) == set(
            NON_COLLECTION_TYPES), f"{list(already_used.keys())} != {NON_COLLECTION_TYPES}"
        already_used = {}
        all_variants_count = len(NON_COLLECTION_TYPES) + len(EMBEDDABLE_NON_COLLECTION_COLUMN_TYPES) * 3
        # <plain variants> + <map variants> + <set variants> + <list variants>
        for n in range(all_variants_count):
            random_type = CQLTypeBuilder.get_random(already_used, allow_levels=1)
            assert random_type is not None, f"Should not return None({random_type}) until end of variants reached"
            random_type.remember_variant(already_used)
        random_type = CQLTypeBuilder.get_random(already_used, allow_levels=1)
        assert random_type is None, "We should reach end of variants here, and therefore None should be returned"

    def test_str_and_remember_forget_variants(self):

        type_int = CQLTypeBuilder('int')
        assert type_int._get_available_variants(
            already_created_info={},
            avoid_types=['int', 'map'],
            allowed_types=['int', 'map', 'list', 'text'],
            allow_levels=1
        ) == ['list', 'text']
        assert type_int._get_available_variants(
            already_created_info={},
            avoid_types=['int'],
            allowed_types=['int', 'map', 'list', 'text'],
            allow_levels=0
        ) == ['text']

        type_map_of_int_int = CQLTypeBuilder('map', key_type=CQLTypeBuilder('int'), value_type=CQLTypeBuilder('int'))
        type_map_of_map_of_int_int = CQLTypeBuilder('map', key_type=type_map_of_int_int, value_type=type_map_of_int_int)

        assert str(type_int) == 'int'
        assert str(type_map_of_int_int) == 'map<int,int>'
        assert str(type_map_of_map_of_int_int) == 'map<frozen<map<int,int>>,frozen<map<int,int>>>'
        already_used_info = {}
        type_int.remember_variant(already_used_info)
        assert already_used_info == {'int': True}, f'{already_used_info}'
        type_map_of_int_int.remember_variant(already_used_info)
        assert already_used_info == {'int': True, 'map': {'key_type': {'int': True}, 'value_type': {'int': True}}}, \
            f'{already_used_info}'
        type_map_of_map_of_int_int.remember_variant(already_used_info)
        assert already_used_info == {
            'int': True, 'map': {
                'key_type': {'int': True, 'map': {
                    'key_type': {'int': True},
                    'value_type': {'int': True}}},
                'value_type': {'int': True, 'map': {
                    'key_type': {'int': True},
                    'value_type': {'int': True}}}}}, \
            f'{already_used_info}'
        type_map_of_map_of_int_int.forget_variant(already_used_info)
        assert already_used_info == {'int': True, 'map': {'key_type': {'int': True},
                                                          'value_type': {'int': True}}}, f'{already_used_info}'
        type_map_of_int_int.forget_variant(already_used_info)
        assert already_used_info == {'int': True}, f'{already_used_info}'
        type_int.forget_variant(already_used_info)
        assert already_used_info == {}, f'{already_used_info}'
