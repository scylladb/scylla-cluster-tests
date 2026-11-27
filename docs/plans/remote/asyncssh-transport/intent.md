# asyncssh SSH transport

**Jira:** SCT-1396

## Problem

The default SSH transport of SCT, `libssh2`, runs on the `ssh2-python`
package. The package has had no commit since 2025-10-12. It ships wheels up
to CPython 3.14 only.

On each new CPython, pip builds `ssh2-python` from source. The source build
links the system `libcrypto`, the same library that the `ssl` module of
Python loads. libssh2 leaves OpenSSL errors on the shared error queue during
key authentication. The next HTTPS call in the same thread then fails. On
Python 3.15 this broke GCE API calls in SCT-1364. PR 16742 works around it
with an `auditwheel` step in the hydra `Dockerfile`. The next CPython needs
the same work again.

macOS arm64 has no usable wheel either. `pyproject.toml` pins a privately
built wheel for it.

The `libssh2` client has no jump host support. Test cases that reach their
nodes through a proxy host must use the `fabric` transport, which runs on
paramiko, a path that SCT is phasing out.

## Who it affects

- Every SCT test run. `libssh2` is the default remoter for every node.
- The hydra image build, on each CPython upgrade.
- Developers on macOS arm64.
- The baremetal pipelines that set `ssh_transport: fabric`.

## Evidence

SCT-1364, a GCE longevity run on the Python 3.15 hydra image:

```
requests.exceptions.SSLError: HTTPSConnectionPool(host='compute.googleapis.com', port=443): Max retries exceeded with url: /compute/v1/projects/sct-project-1/aggregated/instances (Caused by SSLError(SSLError(1, '[OSSL_DECODER] unsupported (_ssl.c:3033)')))
```

- The workaround: https://github.com/scylladb/scylla-cluster-tests/pull/16742
- Upstream issues: https://github.com/ParallelSSH/ssh2-python/issues/245
  (no cp315 wheels) and https://github.com/ParallelSSH/ssh2-python/issues/246
  (shared `libcrypto` error queue).
- PyPI `ssh2-python` 1.2.0.post1 wheel tags: cp38 to cp314, pp311.
- Candidate survey: https://claude.ai/artifact/1b1NhLpotbGdq178vwhGG2

## Definition of done

- A test case can select an SSH transport that needs no native SSH package.
  A new CPython runs it with no source build and no `Dockerfile` step.
- The transport runs commands with the same results, the same exceptions,
  and the same retries as `libssh2`. The remoter integration tests pass on it
  unchanged.
- Measured against `libssh2` on the same hosts, at 60 and 200 parallel
  sessions, the transport stays within the thresholds that the spec sets.
- The transport can reach a node through a jump host.

## Out of scope

- File copy. `send_files` and `receive_files` keep using the system `rsync`
  and `scp`.
- The SCT agent REST runner.
- Making the new transport the default.
- Removing `libssh2`, `ssh2-python`, or `fabric`. A later change does it after
  the rollout.
