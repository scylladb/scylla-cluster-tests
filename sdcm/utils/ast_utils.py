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
# Copyright (c) 2025 ScyllaDB

import ast


class BooleanEvaluator(ast.NodeVisitor):
    # NOTE: based example from https://stackoverflow.com/a/70889491/459189

    def __init__(self, context: dict = None):
        """
        Create a new Boolean Evaluator.
        If you want to allow named variables, give the constructor a
        dictionary which maps names to values. Any name not in the
        dictionary will provoke a NameError exception, but you could
        use a defaultdict to provide a default value (probably False).
        You can modify the symbol table after the evaluator is
        constructed, if you want to evaluate an expression with different
        values.
        """
        self.context = {} if context is None else context

    # Expression is the top-level AST node if you specify mode='eval'.
    # That's not made very clear in the documentation. It's different
    # from an Expr node, which represents an expression statement (and
    # there are no statements in a tree produced with mode='eval').
    def visit_Expression(self, node):
        return self.visit(node.body)

    # 'and' and 'or' are BoolOp, and the parser collapses a sequence of
    # the same operator into a single AST node. The class of the .op
    # member identifies the operator, and the .values member is a list
    # of expressions.
    def visit_BoolOp(self, node):
        if isinstance(node.op, ast.And):
            return all(self.visit(c) for c in node.values)
        elif isinstance(node.op, ast.Or):
            return any(self.visit(c) for c in node.values)
        else:
            # This "shouldn't happen".
            raise NotImplementedError(node.op.__doc__ + " Operator")

    # 'not' is a UnaryOp. So are a number of other things, like unary '-'.
    def visit_UnaryOp(self, node):
        if isinstance(node.op, ast.Not):
            return not self.visit(node.operand)
        else:
            # This error can happen. Try using the `~` operator.
            raise NotImplementedError(node.op.__doc__ + " Operator")

    # Name is a variable name. Technically, we probably should check the
    # ctx member, but unless you decide to handle the walrus operator you
    # should never see anything other than `ast.Load()` as ctx.
    # I didn't check that the symbol table contains a boolean value,
    # but you could certainly do that.
    def visit_Name(self, node):
        return self.context.get(node.id, False)

    # The only constants we're really interested in are True and False,
    # but you could extend this to handle other values like 0 and 1
    # if you wanted to be more liberal
    def visit_Constant(self, node):
        if isinstance(node.value, bool):
            return node.value
        else:
            # I avoid calling str on the value in case that executes
            # a dunder method.
            raise ValueError("non-boolean value")

    # The `generic_visit` method is called for any AST node for
    # which a specific visitor method hasn't been provided.
    def generic_visit(self, node):
        raise RuntimeError("non-boolean expression")
