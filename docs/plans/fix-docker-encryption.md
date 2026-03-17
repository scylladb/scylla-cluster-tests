# Fix Docker Backend Client/Server Encryption Support

## 1. Problem Statement

The Docker backend does not properly support `client_encrypt` and `server_encrypt` configuration options. Two root causes:

1. **Missing certificate generation in `ScyllaDockerCluster.node_setup()`**: Unlike cloud backends (AWS/GCE/Azure) which call `_generate_db_node_certs()` during `node_setup()` (see `sdcm/cluster.py:5639-5640`), the Docker backend's `ScyllaDockerCluster.node_setup()` (`sdcm/cluster_docker.py:437-444`) skips certificate generation entirely.

2. **Broken `send_files()` directory copy in `DockerCmdRunner`**: The `_create_tar_stream()` method (`sdcm/remote/docker_cmd_runner.py:154-168`) uses `src_path.parent` as the relative-to base when creating tar archives for directories, which nests the source directory inside the destination instead of copying its contents. This breaks `install_client_certificate()` which relies on correct directory-level file transfer.

3. **Path handling in `install_client_certificate()`**: The function (`sdcm/provision/helpers/certificate.py:78-95`) does not account for Docker's `send_files` behavior when copying subdirectories (node-specific certs and client certs).

These issues were identified in PR #13065 (issue #7287).

## 2. Current State

- **`ScyllaDockerCluster.node_setup()`** (`sdcm/cluster_docker.py:437-444`): Calls `config_setup()` and `restart_scylla()` but does not generate SSL certificates.
- **Cloud backend `node_setup()`** (`sdcm/cluster.py:5639-5640`): Calls `_generate_db_node_certs(node)` when `server_encrypt` or `client_encrypt` is enabled.
- **`_generate_db_node_certs()`** (`sdcm/cluster.py:5816-5828`): Creates DB cert, client-facing cert, and copies CA + JKS truststore to the node's `ssl_conf_dir`.
- **`DockerCmdRunner._create_tar_stream()`** (`sdcm/remote/docker_cmd_runner.py:154-168`): For directories, uses `file_path.relative_to(src_path.parent)` which includes the source directory name in the archive, causing files to land in a nested subdirectory.
- **`install_client_certificate()`** (`sdcm/provision/helpers/certificate.py:78-95`): Copies node-specific SSL dir and client dir to `/tmp/ssl_conf` then moves to `/etc/scylla/ssl_conf`. The `send_files` calls with trailing slashes don't work correctly with Docker's tar-based transfer.
- **`LoaderSetDocker.node_setup()`** (`sdcm/cluster_docker.py:586-597`): Calls `node.config_client_encrypt()` which invokes `install_client_certificate()`, but loader certs are not generated first (no `_generate_loader_certs()` call).

## 3. Goals

1. SSL certificates are generated for Docker DB nodes when `server_encrypt` or `client_encrypt` is enabled
2. SSL certificates are generated for Docker loader nodes when `client_encrypt` is enabled
3. `DockerCmdRunner.send_files()` correctly handles directory copies (trailing slash = copy contents only)
4. `install_client_certificate()` paths work correctly with Docker's tar-based file transfer
5. Existing unit tests pass; new unit tests cover the `_create_tar_stream` fix

## 4. Implementation Phases

### Phase 1: Fix `DockerCmdRunner._create_tar_stream()` directory handling

**Importance:** Critical — blocks all other fixes

**Changes:**
- `sdcm/remote/docker_cmd_runner.py`: In `_create_tar_stream()`, when `src` ends with `/`, use `src_path` (not `src_path.parent`) as the relative-to base so only directory contents are archived. When `src` does not end with `/`, keep the current behavior (include the directory itself).

**Definition of Done:**
- [ ] Directory copy with trailing slash copies contents only
- [ ] Directory copy without trailing slash copies directory itself
- [ ] Unit test verifies both behaviors

### Phase 2: Fix `install_client_certificate()` path handling

**Importance:** Critical — needed for correct cert installation in Docker containers

**Changes:**
- `sdcm/provision/helpers/certificate.py`: Adjust `install_client_certificate()` to explicitly create subdirectories and send files with correct paths for both SSH-based and Docker-based remoters.

**Definition of Done:**
- [ ] Node-specific certs land in `/etc/scylla/ssl_conf/` (not nested)
- [ ] Client certs land in `/etc/scylla/ssl_conf/client/`

### Phase 3: Add SSL certificate generation to Docker backend

**Importance:** Critical — the main feature

**Changes:**
- `sdcm/cluster_docker.py`: Add certificate generation to `ScyllaDockerCluster.node_setup()` (matching `_generate_db_node_certs` pattern from `cluster.py:5816-5828`)
- `sdcm/cluster_docker.py`: Add certificate generation to `LoaderSetDocker.node_setup()` (matching `_generate_loader_certs` pattern from `cluster.py:6221-6232`)

**Definition of Done:**
- [ ] DB nodes get server cert, client-facing cert, CA cert, and JKS truststore when encryption is enabled
- [ ] Loader nodes get client cert, CA cert, JKS truststore, and PKCS12 keystore when `client_encrypt` is enabled

### Phase 4: Unit tests

**Changes:**
- `unit_tests/test_docker_cmd_runner.py`: Add tests for `_create_tar_stream` with and without trailing slash on source directory

**Definition of Done:**
- [ ] Tests verify correct archive structure for directory with trailing slash
- [ ] Tests verify correct archive structure for directory without trailing slash
- [ ] All existing tests pass

## 5. Testing Requirements

- **Unit tests**: Verify `_create_tar_stream` produces correct tar archive paths for both trailing-slash and non-trailing-slash source directories
- **Manual testing**: Run a Docker backend test with `client_encrypt: true` to verify SSL handshake succeeds

## 6. Success Criteria

- `uv run sct.py unit-tests` passes
- `uv run sct.py pre-commit` passes
- Docker backend can start with `client_encrypt: true` without SSL errors

## 7. Risk Mitigation

| Risk | Likelihood | Impact | Mitigation |
|------|-----------|--------|------------|
| `send_files` fix changes behavior for non-encryption uses | Medium | High | Only change behavior when src ends with `/`; add unit tests for both cases |
| `install_client_certificate` path changes break SSH-based backends | Low | High | The function uses `remoter.send_files()` which has rsync-like semantics on SSH; test with Docker backend specifically |
| Certificate generation order matters (CA must exist first) | Low | Medium | CA is created in `tester.py` during test init, before `node_setup()` runs |
