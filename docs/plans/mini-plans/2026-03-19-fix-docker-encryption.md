# Mini-Plan: Fix Docker Backend SSL Certificate Generation and File Transfer

**Date:** 2026-03-19
**Estimated LOC:** 240
**Related PR:** #14102

## Problem

The Docker backend does not support `client_encrypt` and `server_encrypt` configuration options. Three root causes: (1) `ScyllaDockerCluster.node_setup()` skips certificate generation unlike cloud backends, (2) `DockerCmdRunner._create_tar_stream()` nests directories incorrectly when copying with trailing slash, and (3) `install_client_certificate()` path handling doesn't account for Docker's tar-based file transfer.

## Approach

- Fix `_create_tar_stream()` in `DockerCmdRunner` to use `src_path` (not `src_path.parent`) as relative-to base when source ends with `/`, so only directory contents are archived
- Adjust `install_client_certificate()` to create subdirectories explicitly and send files with correct paths for Docker-based remoters
- Add SSL certificate generation to `ScyllaDockerCluster.node_setup()` matching `_generate_db_node_certs` pattern from `cluster.py`
- Add SSL certificate generation to `LoaderSetDocker.node_setup()` matching `_generate_loader_certs` pattern
- Add unit tests for `_create_tar_stream` with and without trailing slash on source directory

## Files to Modify

- `sdcm/remote/docker_cmd_runner.py` -- Fix `_create_tar_stream()` directory handling for trailing-slash sources
- `sdcm/provision/helpers/certificate.py` -- Adjust `install_client_certificate()` path handling for Docker
- `sdcm/cluster_docker.py` -- Add cert generation to `ScyllaDockerCluster.node_setup()` and `LoaderSetDocker.node_setup()`
- `unit_tests/test_docker_tar_stream.py` -- (new file) Tests for tar stream directory copy behavior

## Verification

- [ ] Unit tests pass: `uv run python -m pytest unit_tests/test_docker_tar_stream.py -v`
- [ ] Full unit test suite passes: `uv run sct.py unit-tests`
- [ ] `uv run sct.py pre-commit` passes
- [ ] Manual: Docker backend test with `client_encrypt: true` completes without SSL errors
