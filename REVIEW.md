# Review guide

This guide applies to each pull request in SCT, from a person or from an
agent. A reviewer works through the three passes in order.

The `code-review` skill in `skills/code-review/` holds the detailed checks
and the real incidents behind them. The standards are in
[docs/INDEX.md](docs/INDEX.md).

## The outcome of a pass

A review states an outcome for each pass: `pass`, or the findings of that
pass. A pass with no outcome did not run. Report the three outcomes together,
in order, so that a reader sees which pass found what.

## Pass 1 — Bugs and logic

- Does the change do what the pull request description says?
- Which input makes it fail? Name one.
- Does a method signature change in a base class reach every override and
  every test stub in `unit_tests/`? This bug passes the linters and the unit
  tests.
- Does the change need the same change on another backend: AWS, GCE, Azure,
  OCI, Docker, K8S, or bare metal?
- Does a new configuration option have a default in `defaults/`?
- Does an error path hide the error? A bare `except` and an ignored return
  value both hide a failure.
- Does a test assert the behavior, or only the implementation?

## Pass 2 — Security

- Does the diff carry a credential, a token, a private key, or an internal
  host name? Credentials go through `KeyStore`.
- Does a remote command or a subprocess call take input that nobody checks?
  Look closely at `sdcm/remote/`.
- Does a change to `.github/workflows/` widen a permission, or run code from
  the pull request head under `pull_request_target`?
- Does a change to `sdcm/provision/security.py` open a port or a network
  range wider than the test needs?
- Does a new dependency arrive without a pin in `pyproject.toml` and
  `uv.lock`?

## Pass 3 — Compliance

- When the pull request carries an idea directory in
  `docs/plans/<feature>/<idea>/`, read its `spec.md`. Does the change stay
  inside the design, the constraints, and the decisions? Does it keep each
  contract that the spec names?
- Does the change build something that the spec does not design? Does each
  design change in the range update the spec?
- Does an `(open)` line remain in the `Decisions` section of the spec?
- When the idea directory holds a `plan.md`, is each box checked? The
  description tells why a box stays open.
- Does a bug fix state its root cause in the commit message and the pull
  request description?
- Does the change follow the standards in `docs/standards/`?
- Do the commit messages follow Conventional Commits, as `commitlint.config.js`
  defines them?
- Does the pull request carry a backport label, and a
  `test-provision-<backend>` label for each backend whose provisioning code
  it touches?

## Volume

Report each finding that changes the code. Group the small findings in one
comment. A review with twenty small findings hides the one that matters.

Findings do not block a merge alone. A maintainer approves the merge.
