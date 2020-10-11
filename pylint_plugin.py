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

# We need this plugin because of two open pylint' issues:
#   * https://github.com/PyCQA/pylint/issues/3882
#   * https://github.com/PyCQA/pylint/issues/3876
# Once these issues will be fixed we can update pylint and remove this plugin.

from astroid import MANAGER, FunctionDef, inference_tip, extract_node
from astroid.brain.brain_typing import TYPING_TYPE_TEMPLATE


TYPING_SPECIAL_FUNCS = frozenset(("NamedTuple", "TypedDict", ))
TYPING_DECORATORS = frozenset(("_SpecialForm", "_LiteralSpecialForm", ))


def register(linter):
    pass


def need_to_infer(node):
    if node.name in TYPING_SPECIAL_FUNCS:
        return True
    if node.decorators and node.decorators.nodes[0].as_string() in TYPING_DECORATORS:
        return True
    return False


def infer_typing_type(node, context=None):
    return extract_node(TYPING_TYPE_TEMPLATE.format(node.name)).infer(context=context)


MANAGER.register_transform(FunctionDef, inference_tip(infer_typing_type), need_to_infer)
