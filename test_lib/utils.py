import typing
import functools
import collections


class __DEFAULT__:
    pass


class __DEFAULT2__:
    pass


def get_data_by_path(
        data: typing.Union[list, dict, object, typing.Type[object]],
        data_path: str,
        default: typing.Any = __DEFAULT__):
    """
    Get data, that could be dict, list, class or instance and go thru it using data_path

    data: list, dict, instance or class
    data_path: string of data path
    detault: if provided, in case of it can't reach data default value will be returned,
      otherwise ValueError will be raised

    data_path example = attr1.[10].attr2 , using this example you will
      get attr2 of 10th entity in the attribute attr1 of the object you provide into data.

    """
    current = data
    for param_name in data_path.split('.'):
        value = __DEFAULT__
        if hasattr(current, param_name):
            value = getattr(current, param_name)
        elif hasattr(current, 'get'):
            value = current.get(param_name, __DEFAULT__)
        elif len(param_name) > 3 and param_name[0] == '[' and param_name[-1] == ']':
            tmp = param_name[1:-1]
            if tmp.isdecimal():
                if hasattr(current, '__getitem__'):
                    tmp = int(param_name[1:-1])
                    if len(current) > tmp:
                        value = current[tmp]
        if value is __DEFAULT__:
            if default is __DEFAULT__:
                raise ValueError(f"Can't find {data_path} in data {data}")
            return default
        current = value
    return current


def get_class_by_path(
        cls: typing.Type[object],
        class_path: str,
        default: typing.Any = __DEFAULT__):
    """
    Get class go thru it's annotations using class_path

    cls: list, dict, instance or class
    class_path: string of data path
    detault: if provided, in case of it can't reach data default value will be returned,
      otherwise ValueError will be raised

    data_path example = attr1.attr2.attr3 , using this example you will
      get attr3 of attr2 of attr1 of the class you provide into cls.

    """
    current = cls
    for param_name in class_path.split('.'):
        value = __DEFAULT__
        if hasattr(current, '__annotations__'):
            value = current.__annotations__.get(param_name, __DEFAULT__)
        if value is __DEFAULT__:
            if default is __DEFAULT__:
                raise ValueError(f"Can't find {class_path} in data {cls.__name__}")
            return default
        current = value
    return current


GroupByType = typing.Dict[typing.Any, typing.Union['GroupByType', 'MagicList']]


class MagicList(list):
    def group_by(
            self,
            data_path,
            default: typing.Any = __DEFAULT__,
            sort_keys=None,
            group_values: dict = None) -> GroupByType:
        """
        Groups list content by data_path, do that iterative if group_values is provided and return resulted dict
        data_path: str,  data path to get group value, look at get_data_by_path for more details
        default: any,  if provided, it is used as default value if it can't reach data.
          if omitted entities with no value are ignored
        sort_keys: int,  if None, keys are not sorted, if -1, sorted in descending order, if 1, sorted in ascending order
        group_values: dict,  if provided, it will call group_by on values with parameters from this dictionary
        """
        output = {}
        for element in self:
            idx = get_data_by_path(element, data_path, __DEFAULT2__)
            if idx is __DEFAULT2__:
                if default is __DEFAULT__:
                    continue
                idx = default
            bucket = output.get(idx, None)
            if bucket is None:
                output[idx] = MagicList([element])
            else:
                output[idx].append(element)
        if group_values:
            if 'data_path' not in group_values:
                raise ValueError("sort_values dict should contains same parameters as needed for sort_by")
            for key, values in list(output.items()):
                output[key] = MagicList(values).group_by(
                    data_path=group_values['data_path'],
                    default=group_values.get('default', __DEFAULT__),
                    sort_keys=group_values.get('sort_keys', None),
                    group_values=group_values.get('group_values', None),
                )
        if sort_keys:
            return collections.OrderedDict(
                sorted(
                    output.items(),
                    reverse=bool(sort_keys < 0),
                    key=lambda x: x[0]
                )
            )
        return output

    def sort_by(self,
                data_path: str,
                default: typing.Any = __DEFAULT__
                ) -> 'MagicList':
        """
        Returns list of same entities sorted by data_path
        data_path: str,  data_path of keys for sorting, look at get_data_by_path for more details
        default: any,  used as default value for entity if it does not hold it,
          if omitted, such entities will be ignored
        """
        target = []
        if default is __DEFAULT__:
            for element in self:
                if get_data_by_path(element, data_path, __DEFAULT2__) is __DEFAULT2__:
                    continue
                target.append(element)
        else:
            target = self
        output = sorted(target, key=functools.partial(get_data_by_path, data_path=data_path, default=default))
        return MagicList(output)

    def remove_where(self, data_path, value, default=__DEFAULT2__):
        to_delete = []
        for element in self:
            if get_data_by_path(element, data_path, default=default) == value:
                to_delete.append(element)
        for element in to_delete:
            self.remove(element)
        return self
