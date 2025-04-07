import random


__ALL__ = ['CQLTypeBuilder', 'ALL_COLUMN_TYPES', 'NOT_EMBEDDABLE_COLUMN_TYPES', 'COLLECTION_COLUMN_TYPES']


ALL_COLUMN_TYPES = ['ascii', 'blob', 'counter', 'decimal', 'duration', 'int', 'map', 'smallint', 'time',
                    'timeuuid', 'uuid', 'varint', 'bigint', 'boolean', 'date', 'double', 'float', 'inet', 'list', 'set',
                    'text', 'timestamp', 'tinyint', 'varchar']
NOT_EMBEDDABLE_COLUMN_TYPES = ['duration', 'counter']
COLLECTION_COLUMN_TYPES = ['set', 'list', 'map']


class CQLTypeBuilder:
    _registered_classes = {}

    def __new__(cls, self_type, *args, **kwargs) -> 'CQLColumnType':
        return cls._create_instance(self_type, *args, **kwargs)

    @classmethod
    def _register_class(cls, registered_class):
        if CQLTypeBuilder._registered_classes.get(registered_class.self_type, None) is not None:
            raise RuntimeError(f"<{cls}> self_type {registered_class.self_type} is already registered")
        CQLTypeBuilder._registered_classes[registered_class.self_type] = registered_class

    @classmethod
    def _get_registered_class(cls, self_type, default=None):
        return cls._registered_classes.get(self_type, default)

    @classmethod
    def _create_instance(cls, self_type, *args, **kwargs):
        target_class = cls._get_registered_class(self_type, CQLColumnType)
        return target_class(self_type, *args, **kwargs)

    @classmethod
    def get_random(cls, already_created_info: dict, avoid_types: list = None,
                   allow_levels: int = 1, allowed_types: list = None, forget_on_exhaust=False) -> 'CQLColumnType':
        return CQLColumnType.get_random(
            already_created_info,
            avoid_types,
            allow_levels,
            allowed_types,
            forget_on_exhaust=forget_on_exhaust)


class CQLColumnType:
    self_type = None
    parent = None

    def __init_subclass__(cls, **__):
        if cls.self_type is None:
            raise ValueError(f"<{str(cls)}> self_type should be defined")
        CQLTypeBuilder._register_class(cls)

    def __init__(self, self_type=None):
        self.self_type = self_type

    def __str__(self):
        return self.self_type

    @staticmethod
    def _get_available_variants(already_created_info, avoid_types=None, allowed_types=None, allow_levels=None):
        if avoid_types is None:
            avoid_types = []
        if allowed_types is None:
            allowed_types = ALL_COLUMN_TYPES
        already_created_types = [e for e in already_created_info.keys() if e not in COLLECTION_COLUMN_TYPES]
        excluded_types = avoid_types + already_created_types
        if allow_levels == 0:
            excluded_types += COLLECTION_COLUMN_TYPES
        return [e for e in allowed_types if e not in excluded_types]

    @classmethod
    def get_random(cls, already_created_info: dict, avoid_types: list = None,
                   allow_levels: int = 1, allowed_types: list = None, forget_on_exhaust=False):
        """
        Randomly generates CQLColumnType instance
        Arguments:
            already_created_info {dict} - Dictionary that stores information about column types to avoid
                , generated by remember_variant,
            avoid_types {list} - List of types to avoid across all levels
            max_level {int} - Maximum level of embedding, i.e. list<int> has 1 level, list<frozen<list<int>>>, has 2
            allowed_types {list} - List of types to allow across all levels
            forget_on_exhaust {bool} - If True it will drop all remembered variants and start over
        return:
            Will return randomly generated instance of CQLColumnType
        """

        while True:
            calculated_available_types = cls._get_available_variants(already_created_info, avoid_types, allowed_types,
                                                                     allow_levels)
            while calculated_available_types:
                self_type = random.choice(calculated_available_types)
                self_bucket = already_created_info.get(self_type, {})
                instance = CQLTypeBuilder._create_instance(self_type)
                if instance._get_random_embedded(self_bucket, avoid_types, allow_levels,
                                                 allowed_types):
                    return instance
                #  It could reach this point only if collection type ran out of choice for subtypes
                #  Remove current collection type from types choice
                calculated_available_types.remove(self_type)
            if not forget_on_exhaust:
                return None
            already_created_info.clear()

    def _get_random_embedded(self,
                             already_created_info: dict, avoid_types: list, allow_levels: int, allowed_types: list):
        return True

    def remember_variant(self, stored_variants):
        """
        Go down thru structure of stored_variants and set mark there based on instance of CQLColumnType
        """
        self_bucket = stored_variants.get(self.self_type, None)
        if self_bucket is None:
            self_bucket = stored_variants[self.self_type] = True
        return self_bucket

    def forget_variant(self, stored_variants):
        """
        Go down thru structure of stored_variants and remove mark about instance of CQLColumnType
        """
        if not stored_variants:
            return
        if self.self_type in stored_variants:
            del stored_variants[self.self_type]


class CQLColumnTypeMap(CQLColumnType):
    self_type = 'map'

    def __new__(cls, self_type=None, key_type=None, value_type=None):
        return super().__new__(cls)

    def __init__(self, self_type=None, key_type=None, value_type=None):
        self.key_type = key_type
        self.value_type = value_type
        super().__init__(self_type)

    def __str__(self):
        if self.key_type.self_type in COLLECTION_COLUMN_TYPES:
            key_type = f'frozen<{self.key_type.__str__()}>'
        else:
            key_type = self.key_type.__str__()
        if self.value_type.self_type in COLLECTION_COLUMN_TYPES:
            value_type = f'frozen<{self.value_type.__str__()}>'
        else:
            value_type = self.value_type.__str__()
        return f'{self.self_type}<{key_type},{value_type}>'

    def _get_random_embedded(self, already_created_info: dict, avoid_types: list, allow_levels: int, allowed_types: list):
        if avoid_types:
            tmp = list(set(avoid_types.copy() + NOT_EMBEDDABLE_COLUMN_TYPES))
        else:
            tmp = NOT_EMBEDDABLE_COLUMN_TYPES
        self.key_type = self.get_random(already_created_info.get('key_type', {}), tmp, allow_levels - 1,
                                        allowed_types)
        self.value_type = self.get_random(already_created_info.get('value_type', {}), tmp, allow_levels - 1,
                                          allowed_types)
        if self.key_type is None or self.value_type is None:
            return False
        return True

    def remember_variant(self, stored_variants):
        """
        Go down thru structure of stored_variants and set mark there based on instance of CQLColumnType
        """
        self_bucket = stored_variants.get(self.self_type, None)
        if self_bucket is None:
            self_bucket = stored_variants[self.self_type] = {}
        key_type_bucket = self_bucket.get('key_type', None)
        if key_type_bucket is None:
            key_type_bucket = self_bucket['key_type'] = {}
        value_type_bucket = self_bucket.get('value_type', None)
        if value_type_bucket is None:
            value_type_bucket = self_bucket['value_type'] = {}
        self.key_type.remember_variant(key_type_bucket)
        self.value_type.remember_variant(value_type_bucket)

    def forget_variant(self, stored_variants):
        """
        Go down thru structure of stored_variants and remove mark about instance of CQLColumnType
        """
        if not stored_variants:
            return
        self_bucket = stored_variants.get(self.self_type, None)
        if self_bucket is None:
            return
        self.key_type.forget_variant(self_bucket.get('key_type', None))
        if self_bucket.get('key_type', None) == {}:
            del self_bucket['key_type']
        self.value_type.forget_variant(self_bucket.get('value_type', None))
        if self_bucket.get('value_type', None) == {}:
            del self_bucket['value_type']
        if stored_variants.get(self.self_type, None) == {}:
            del stored_variants[self.self_type]


class CQLColumnTypeList(CQLColumnType):
    self_type = 'list'

    def __new__(cls, self_type=None, element_type=None):
        return super().__new__(cls)

    def __init__(self, self_type=None, element_type=None):
        self.element_type = element_type
        super().__init__(self_type)

    def __str__(self):
        if self.element_type.self_type in COLLECTION_COLUMN_TYPES:
            element_type = f'frozen<{self.element_type.__str__()}>'
        else:
            element_type = self.element_type.__str__()
        return f'{self.self_type}<{element_type}>'

    def _get_random_embedded(self, already_created_info: dict, avoid_types: list, allow_levels: int, allowed_types: list):
        if avoid_types:
            tmp = list(set(avoid_types.copy() + NOT_EMBEDDABLE_COLUMN_TYPES))
        else:
            tmp = NOT_EMBEDDABLE_COLUMN_TYPES
        self.element_type = self.get_random(already_created_info.get('element_type', {}), tmp, allow_levels - 1,
                                            allowed_types)
        if self.element_type is None:
            return False
        return True

    def remember_variant(self, stored_variants):
        """
        Go down thru structure of stored_variants and set mark there based on instance of CQLColumnType
        """
        self_bucket = stored_variants.get(self.self_type, None)
        if self_bucket is None:
            self_bucket = stored_variants[self.self_type] = {}
        element_type_bucket = self_bucket.get('element_type', None)
        if element_type_bucket is None:
            element_type_bucket = self_bucket['element_type'] = {}
        self.element_type.remember_variant(element_type_bucket)

    def forget_variant(self, stored_variants):
        """
        Go down thru structure of stored_variants and remove mark about instance of CQLColumnType
        """
        if not stored_variants:
            return
        self_bucket = stored_variants.get(self.self_type, None)
        if self_bucket is None:
            return
        self.element_type.forget_variant(self_bucket.get('element_type', None))
        if self_bucket.get('element_type', None) == {}:
            del self_bucket['element_type']
        if stored_variants.get(self.self_type, None) == {}:
            del stored_variants[self.self_type]


class FieldTypeSetDefinition(CQLColumnTypeList):
    self_type = 'set'
