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
# Copyright (c) 2020 ScyllaDB

from typing import Optional, Any, Dict
import json
import enum


class DefaultValue:
    pass


__builtin__ = [].__class__.__module__


def is_builtin(obj):
    return obj.__module__ == __builtin__


def _init_invoke_result(instance_type, children):
    return False, instance_type(
        connection=None,
        **{attr_name: children.get(attr_name, None) for attr_name in ['stderr', 'stdout', 'exited']}
    )


class PicklerAction(enum.Enum):
    PASSTHROUGH = 1


class Pickler:
    class_info = {
        'fabric.runners.Result': {
            'init_args': {
                'stderr': PicklerAction.PASSTHROUGH,
                'stdout': PicklerAction.PASSTHROUGH,
                'exited': PicklerAction.PASSTHROUGH,
                'connection': None
            },
            'instance_attrs': {
                'exit_status': PicklerAction.PASSTHROUGH
            }
        },
        'sdcm.coredump.CoreDumpInfo': {
            'init_args': {
                'process_retry': 0,
                'node': None,
                '*': PicklerAction.PASSTHROUGH,
            },
            'instance_attrs': {
                'event_timestamp': None,
            }
        },
        'invoke.exceptions.UnexpectedExit': {
            'init_args': {
                'result': PicklerAction.PASSTHROUGH,
                'reason': PicklerAction.PASSTHROUGH
            }
        }
    }

    @staticmethod
    def get_class_path(instance_class):
        if is_builtin(instance_class):
            return f'{instance_class.__name__}'
        return f'{instance_class.__module__}.{instance_class.__name__}'

    @staticmethod
    def import_and_return(path):
        global_vars = globals()
        tclass = global_vars.get(path, None)
        if tclass is not None:
            return tclass
        paths = path.split('.')
        global_vars[path] = lib = getattr(__import__(
            '.'.join(paths[0:-1]), globals(), locals(), fromlist=[paths[-1]], level=0), paths[-1])
        return lib

    @classmethod
    def _get_attrs_by_class(cls, instance_class: str, attr_type=None) -> Optional[Dict[str, Any]]:
        info = cls.class_info.get(instance_class, None)
        if info is None:
            return None
        if attr_type is None:
            output = {}
            output.update(info.get('init_args', {}))
            output.update(info.get('instance_attrs', {}))
            return output
        return info.get(attr_type, None)

    @classmethod
    def _to_data_object(cls, obj):
        instance_class = cls.get_class_path(type(obj))
        result = {
            '__instance__': instance_class
        }
        attribute_action = cls._get_attrs_by_class(instance_class)
        if attribute_action is None:
            attribute_action = {}
        default_action = attribute_action.get('*', DefaultValue)
        for attr_name, attr_value in obj.__dict__.items():
            attr_action = attribute_action.get(attr_name, default_action)
            if not isinstance(attr_action, PicklerAction):
                continue
            result[attr_name] = cls.to_data(attr_value)
        return result

    @classmethod
    def _to_data_dict(cls, obj: dict) -> dict:
        output = {}
        for attr_name, attr_value in obj.items():
            output[attr_name] = cls.to_data(attr_value)
        return output

    @classmethod
    def _to_data_list(cls, obj: list) -> list:
        output = []
        for attr_value in obj:
            output.append(cls.to_data(attr_value))
        return output

    @classmethod
    def to_data(cls, obj):
        if isinstance(obj, (str, type(None), int, float)):
            return obj
        if isinstance(obj, dict):
            return cls._to_data_dict(obj)
        if isinstance(obj, list):
            return cls._to_data_list(obj)
        if isinstance(obj, tuple):
            return tuple(cls._to_data_list(obj))
        if isinstance(obj, type):
            return {'__class__': obj.__name__}
        return cls._to_data_object(obj)

    @classmethod
    def from_data(cls, data):
        if isinstance(data, (str, type(None), int, float)):
            return data
        if isinstance(data, dict):
            return cls._from_data_dict(data)
        if isinstance(data, list):
            return cls._from_data_list(data)
        raise RuntimeError(f"Wrong data type '{type(data)}' ")

    @classmethod
    def _from_data_iterate_attrs(cls, instance_type, obj: dict, args_bucket_name: str, default_action=None):
        action_map = cls._get_attrs_by_class(instance_type, args_bucket_name)
        if not action_map:
            raise RuntimeError("Unknown class, please, add this class to 'class_info'")

        default_action = action_map.get('*', default_action)
        if not isinstance(obj, dict):
            obj = obj.__dict__
        for attr_name, attr_value in obj.items():
            attr_action = action_map.get(attr_name, default_action)
            if attr_action is PicklerAction.PASSTHROUGH:
                yield attr_name, cls.from_data(attr_value)
                continue
        for attr_name, attr_action in action_map.items():
            if attr_name != '*' and not isinstance(attr_action, PicklerAction):
                yield attr_name, attr_action

    @classmethod
    def _from_data_dict(cls, obj: dict) -> dict:
        obj = obj.copy()
        tmp = obj.pop('__class__', None)
        if tmp is not None:
            return globals()[tmp]
        instance_type = obj.pop('__instance__', None)
        if instance_type is None:
            return {attr_name: cls.from_data(attr_value) for attr_name, attr_value in obj.items()}
        init_args = {}
        for attr_name, attr_value in cls._from_data_iterate_attrs(instance_type, obj, 'init_args', None):
            init_args[attr_name] = attr_value
        instance_class = cls.import_and_return(instance_type)
        instance = instance_class(**init_args)
        for attr_name, attr_value in cls._from_data_iterate_attrs(instance_type, obj, 'init_args', PicklerAction.PASSTHROUGH):
            setattr(instance, attr_name, attr_value)
        return instance

    @classmethod
    def _from_data_list(cls, obj: list) -> list:
        output = []
        for attr_value in obj:
            output.append(cls.from_data(attr_value))
        return output

    @classmethod
    def load_from_file(cls, filepath):
        with open(filepath, encoding="utf-8") as file:
            return cls.from_data(json.load(file))

    @classmethod
    def load_data_from_file(cls, filepath):
        with open(filepath, encoding="utf-8") as file:
            return json.load(file)

    @classmethod
    def save_to_file(cls, filepath, data):
        with open(filepath, 'w', encoding="utf-8") as file:
            return json.dump(cls.to_data(data), file)

    _init_by_type = {
        'fabric.runners.Result': _init_invoke_result
    }
