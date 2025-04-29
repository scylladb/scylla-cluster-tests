from test_lib.utils import get_data_by_path


class __DEFAULT__:
    pass


class ClassBase:
    """
    This class that is meant to be used as base for class that could be stored or loaded (in ES or any other backend)
    """
    _es_data_mapping = {}
    _data_type = None

    def __init__(self, es_data=None, **kwargs):
        if es_data:
            self.load_from_es_data(es_data)
        if kwargs:
            self.load_kwargs(kwargs)

    def load_kwargs(self, kwargs):
        errors = []
        for data_name, value in kwargs.items():
            data_type = self.__annotations__.get(data_name, None)
            if data_type is None:
                errors.append(f'Wrong {data_name} attribute was provided')
                continue
            if not isinstance(value, data_type):
                errors.append(f'Wrong {data_name} attribute value was provided')
                continue
            setattr(self, data_name, value)
        if errors:
            raise ValueError(
                f"Following errors occurred during class {self.__class__.__name__} initialization: \n" +
                "\n".join(errors)
            )

    def load_from_es_data(self, es_data):
        """
        Fills instance data with data from ES
        """
        if not isinstance(es_data, dict):
            raise ValueError(f"Class {self.__class__.__name__} can be loaded only from dict")
        data_mapping = self._es_data_mapping
        for data_name, data_type in self.__annotations__.items():
            data_path = data_mapping.get(data_name, __DEFAULT__)
            if data_path is __DEFAULT__:
                value = es_data.get(data_name, __DEFAULT__)
            elif data_path == '':
                value = es_data
            else:
                value = get_data_by_path(es_data, data_path=data_path, default=__DEFAULT__)
            if value is __DEFAULT__:
                continue
            self._apply_data(data_name, data_type, value)

    def save_to_es_data(self):
        """
        Represents contents of the instance as ES data according to _es_data_mappings
        """
        output = {}

        def data_cb(data_instance, current_instance, data_path, es_data_path, is_edge):
            if is_edge:
                if isinstance(data_instance, ClassBase):
                    value = data_instance.save_to_es_data()
                else:
                    value = data_instance
                output['.'.join(es_data_path)] = value
        self._iterate_data(data_cb)
        return output

    def _apply_data(self, data_name, data_type, value):
        setattr(self, data_name, data_type(value))

    def is_valid(self):
        for data_name in self.__annotations__.keys():
            default = getattr(self.__class__, data_name)
            value = getattr(self, data_name, None)
            if value is default:
                return False
            elif isinstance(value, ClassBase):
                if not value.is_valid():
                    return False
        return True

    @classmethod
    def _get_all_es_data_mapping(cls, max_level=10) -> dict:
        """
        Returns dictionary where keys are all possible class data paths and values are related ES data paths
        """

        if max_level == 0:
            return {}
        output = {}
        for data_name, data_type in cls.__annotations__.items():
            data_path = cls._es_data_mapping.get(data_name, __DEFAULT__)
            if data_path is __DEFAULT__:
                data_path = data_name
            # Set data
            if isinstance(data_type, type) and issubclass(data_type, ClassBase):
                if data_type.load_from_es_data is not cls.load_from_es_data:
                    # No mapping if custom loader defined
                    child_data_mapping = {}
                else:
                    child_data_mapping = data_type._get_all_es_data_mapping(
                        max_level=max_level-1)
                if not child_data_mapping:
                    output[data_name] = data_path
                    continue
                for child_data_name, child_data_path in child_data_mapping.items():
                    if data_path:
                        output[f'{data_name}.{child_data_name}'] = f'{data_path}.{child_data_path}'
                    else:
                        output[f'{data_name}.{child_data_name}'] = child_data_path
            else:
                output[data_name] = data_path
        return output

    def _iterate_data(self, callback, data_path=None, es_data_path=None):
        """
        Iterate all data in the instance by calling callback function
        """
        if data_path is None:
            data_path = []
        if es_data_path is None:
            es_data_path = []
        instances = [(self, data_path, es_data_path)]
        while instances:
            current_instance, data_path, es_data_path = instances.pop()
            for data_name, data_instance in current_instance.__dict__.items():
                if current_instance.__annotations__.get(data_name, None) is None:
                    continue
                es_data_name = current_instance._es_data_mapping.get(
                    data_name, None)
                if es_data_name is None:
                    es_data_name = es_data_path + [data_name]
                elif es_data_name == '':
                    es_data_name = es_data_path
                else:
                    es_data_name = es_data_path + es_data_name.split('.')
                if not isinstance(data_instance, ClassBase) \
                        or data_instance.__class__.load_from_es_data is not ClassBase.load_from_es_data:
                    callback(data_instance, current_instance, data_path + [data_name], es_data_name, True)
                    continue
                if callback(data_instance, current_instance, data_path + [data_name], es_data_name, False):
                    instances.insert(0, (data_instance, data_path + [data_name], es_data_name))

    def _get_es_data_path_and_values_from_patterns(self, data_patterns: list, flatten=False):
        """
        Reads data patterns and builds dictionary of es data paths as keys and instance values as values
        If flatten is True, it will produce one level dictionary,
            otherwise each level of data path will be represented by one level in dictionary
        """
        data_patterns_split = []
        for data_pattern in data_patterns:
            data_patterns_split.append(data_pattern.split('.'))

        output = {}

        def data_cb(data_instance, current_instance, data_path, es_data_path, is_edge):
            final_return = False
            for data_pattern_split in data_patterns_split:
                to_add = len(data_pattern_split) == len(data_path)
                to_return = False
                for num, data_pattern_part in enumerate(data_pattern_split):
                    if num >= len(data_path):
                        to_add = False
                        final_return = True
                        break
                    data_path_part = data_path[num]
                    if data_pattern_part == '*':
                        to_add = is_edge
                        to_return = True
                        break
                    if data_pattern_part != data_path_part:
                        to_add = False
                        break
                if to_add:
                    if isinstance(data_instance, ClassBase):
                        result = data_instance.save_to_es_data()
                    else:
                        result = data_instance
                    current_output = output
                    if flatten:
                        current_output['.'.join(es_data_path)] = result
                    else:
                        for es_data_path_part in es_data_path[:-1]:
                            new_current_output = current_output.get(es_data_path_part, __DEFAULT__)
                            if new_current_output is __DEFAULT__:
                                current_output[es_data_path_part] = {}
                                current_output = current_output[es_data_path_part]
                            else:
                                current_output = new_current_output
                        current_output[es_data_path[-1]] = result
                if to_return:
                    return True
            return final_return
        self._iterate_data(data_cb)
        return output
