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
# Copyright (c) 2026 ScyllaDB

"""Cross-field validation plumbing for the configuration mixins.

The contract, in one sentence:

    Every mutation of an ``SCTConfiguration`` leaves it satisfying every cross-field rule, or
    raises and changes nothing.

A single assignment validates immediately.  ``SCTConfiguration.update`` applies every key first and
validates once at the end, so options that are only *jointly* valid -- ``authenticator`` with its
user and password, say -- can be set together.  If it raises, the configuration is unchanged.

**Why any of this is needed.**  ``SCTConfiguration`` sets ``validate_assignment=True``, and its
``__init__`` is a loader: it builds an all-defaults model and then merges backend defaults, user
YAML, region data and environment variables into it one field assignment at a time -- 242 of them
for a minimal docker configuration.  Each assignment re-runs every ``@model_validator(mode="after")``
on the class, against a half-loaded configuration that is not yet meaningful.  So a rule needs a way
to stay quiet until the thing it judges exists.

`deferred_checks` is that window, and `cross_field_check` is what a mixin writes instead of a raw
model validator so it observes the window without any per-rule boilerplate.

**The flag defaults to False -- checks ON.**  This is deliberate and is the one thing
`PR #14124 <https://github.com/scylladb/scylla-cluster-tests/pull/14124>`_ had backwards.  It
guarded its validators with an instance flag defaulting to *off*, flipped at the end of ``__init__``
-- so ``model_validate``, ``model_copy``, ``model_construct`` and plain keyword construction, none
of which run ``__init__``, skipped every rule forever and silently.  Defaulting to on means a
construction path nobody anticipated gets the checks rather than the silence.

The flag is a `contextvars.ContextVar`, so nested windows unwind correctly and a worker thread
started inside a window sees checks *on* -- failing loudly rather than skipping quietly.  It is
module-global rather than per-instance: two configurations loading concurrently in one thread would
share the window.  Nothing does that today.
"""

import contextvars
import functools
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from typing import TypeVar

from pydantic import BaseModel, model_validator

_ModelT = TypeVar("_ModelT", bound=BaseModel)

#: True only inside `deferred_checks`.  False -- checks enabled -- is the default on purpose; see
#: the module docstring.
_checks_deferred: contextvars.ContextVar[bool] = contextvars.ContextVar(
    "sct_config_checks_deferred", default=False
)


def checks_deferred() -> bool:
    """True while a `deferred_checks` window is open on this context."""
    return _checks_deferred.get()


@contextmanager
def deferred_checks() -> Iterator[None]:
    """Suspend `cross_field_check` rules for the duration of a multi-field mutation.

    For loading and bulk updates, where the intermediate states are not configurations anyone
    should be judging.  The caller is responsible for running the rules once on exit, via
    `run_cross_field_checks` -- this window suspends them, it does not schedule them.

    Nests correctly: the flag is restored to whatever it was, not unconditionally to False.
    """
    token = _checks_deferred.set(True)
    try:
        yield
    finally:
        _checks_deferred.reset(token)


def cross_field_check(func: Callable[[_ModelT], _ModelT]) -> Callable[[_ModelT], _ModelT]:
    """Declare a cross-field rule on a configuration mixin.

    Writes the same thing a ``@model_validator(mode="after")`` would, minus the boilerplate for
    observing `deferred_checks`.  The rule may read any option on the assembled model, including
    ones owned by other mixins -- ``unit_tests/unit/config/test_mixin_validators.py`` pins that
    down -- so a genuinely cross-domain rule does not have to go back to a central function.

    Three requirements, each of which fails quietly rather than loudly if ignored:

    * **It must not assign to an option.**  Under ``validate_assignment=True`` an assignment inside
      a validator re-enters the whole chain.  A rule that normalises a value belongs in a
      ``@field_validator``, or -- when it has to see several options to decide -- in a plain
      ``@model_validator(mode="after")`` written to be idempotent, so the re-entry settles on the
      second pass instead of recursing.  Such a rule must *not* use this decorator: it has to run
      even while deferred, or the value it forces would never be forced during loading.
    * **It must not depend on another rule having run.**  Validators declared on mixins run in
      reverse ``CONFIG_GROUPS`` order, which is deterministic but is an implementation detail of
      how Pydantic walks the MRO.  Where one rule genuinely must follow another, declare the later
      one on ``SCTConfiguration`` itself -- own-class validators run last.
    * **Its method name must be unique across every mixin.**  Pydantic keys validators by class
      attribute name, so two mixins declaring the same name means one of them silently disappears.
      Name them ``check_<domain>_<rule>``; a guard test enforces uniqueness.
    """

    @functools.wraps(func)
    def wrapper(self: _ModelT) -> _ModelT:
        if _checks_deferred.get():
            return self
        return func(self)

    return model_validator(mode="after")(wrapper)


def run_cross_field_checks(model: _ModelT) -> _ModelT:
    """Run every ``mode="after"`` validator on an existing instance, once, in place.

    ``model_validate`` on an instance re-enters the ``function-after`` schemas that wrap the model
    schema, while the model schema itself short-circuits on an isinstance check -- because
    ``revalidate_instances`` is left at its ``'never'`` default.  So the rules run without copying
    the object, without re-coercing any field, and without disturbing the private attributes or the
    record of which fields were explicitly set.  It returns the very same object it was given.

    Two things would break this, both pinned by tests:

    * Setting ``revalidate_instances`` anywhere in the model configuration.  That turns this into a
      deep re-validation of every field and the mechanism collapses.
    * Reaching for ``validate_python(model.__dict__, self_instance=model)`` instead.  Roughly a
      quarter of the options are annotated as non-optional while holding no value, so that path
      raises for a config that is otherwise perfectly fine.
    """
    if _checks_deferred.get():
        raise RuntimeError("run_cross_field_checks() called inside a deferred_checks() window")
    return type(model).model_validate(model)
