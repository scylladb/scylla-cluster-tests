# SSL/TLS on the Docker backend

The Docker backend supports both `client_encrypt` (client-to-node TLS) and `server_encrypt`
(internode TLS), the same two SCT config options used by the cloud backends. Getting there
required Docker-specific handling in three places — certificate generation during node setup,
the tar-stream file transfer used instead of SCP, and the destination paths
`install_client_certificate()` sends to.

This page describes how those pieces fit together and what to look at when TLS fails on the
Docker backend.

## Enabling encryption

```yaml
client_encrypt: true          # client-to-node TLS
server_encrypt: true          # internode TLS
internode_encryption: 'all'   # required for server_encrypt to reach scylla.yaml
```

Additional options that apply to Docker exactly as they do elsewhere:

| Option | Default | Effect |
|---|---|---|
| `peer_verification` | `true` | Stress tools verify the server hostname against the certificate |
| `client_encrypt_mtls` | `false` | Scylla requires a client certificate on the CQL port |
| `server_encrypt_mtls` | `false` | Scylla requires a peer certificate for internode traffic |

`server_encrypt` alone is not enough: `ScyllaYamlCertificateAttrBuilder.server_encryption_options`
returns `None` unless `internode_encryption` is also set, so `server_encryption_options` never
lands in `scylla.yaml`.

## Where certificates come from

Everything is generated on the SCT host, under `data_dir/ssl_conf/`, before any node is touched:

1. `ClusterTester.__init__` calls `create_ca(self.localhost)` when `client_encrypt`,
   `server_encrypt`, or `agent.tls` is set. This writes `ca.pem` / `ca.key` and imports the CA
   into a Java truststore (`truststore.jks`) using a throwaway `java` container — the JKS is what
   cassandra-stress consumes.
2. With `client_encrypt` on, `update_cqlshrc()` rewrites `data_dir/ssl_conf/client/cqlshrc` to
   point at the in-node paths under `/etc/scylla/ssl_conf`.
3. Per-node material goes to `data_dir/ssl_conf/<node-ip>/`, which is what `node.ssl_conf_dir`
   resolves to.

At teardown `cleanup_ssl_config()` removes every per-node directory, keeping only `client/` and
`example/`.

### DB nodes

`ScyllaDockerCluster.node_setup()` mirrors the cloud path in `sdcm/cluster.py`:

```python
if any([self.params.get("server_encrypt"), self.params.get("client_encrypt")]):
    self._generate_db_node_certs(node)
    install_client_certificate(node.remoter, node.ip_address, force=True)
```

`_generate_db_node_certs()` produces, in `node.ssl_conf_dir`:

- `db.crt` / `db.key` / `db.csr` — the server certificate (the CSR is kept because some scenarios
  re-sign it later via `update_certificate()`)
- `client-facing.crt` / `client-facing.key` — the certificate Scylla presents on the CQL port
- copies of `ca.pem` and `truststore.jks`

The SANs come from `BaseNode.create_node_certificate()`: the node's IP addresses plus
`public_dns_name` / `private_dns_name`. On the Docker backend both DNS names fall back to
`BaseNode.name` (the container name) and both IPs are the container's bridge address, since
`DockerNode._get_private_ip_address()` returns the public one.

The explicit `install_client_certificate(..., force=True)` call is the Docker-specific part.
On cloud backends the certificates reach the node lazily, through
`ScyllaYamlCertificateAttrBuilder._ssl_files_path` while `scylla.yaml` is being rendered. That
still happens on Docker, but the `force=True` call up front guarantees the container has the
files before `config_setup()` runs, rather than relying on the builder's cached property firing
at the right moment.

### Loader nodes

Docker loaders are not containers. Since "use local runner for Docker backend loaders instead of
containers", `DockerLoaderNode` runs on the host through `LOCALRUNNER` and stress tools are
launched as `RemoteDocker` containers against the local Docker daemon.

`LoaderSetDocker.node_setup()` therefore only generates certificates and skips
`config_client_encrypt()` entirely:

```python
def node_setup(self, node: DockerLoaderNode, verbose=False, timeout=3600, **kwargs):
    if self.params.get("client_encrypt"):
        self._generate_loader_certs(node)
```

`_generate_loader_certs()` writes `test.crt` / `test.key`, copies `ca.pem` and `truststore.jks`,
and exports a PKCS12 keystore (`keystore.p12`) for cassandra-stress mTLS. Note the loader's
`ssl_conf_dir` is keyed on `127.0.0.1`, because `DockerLoaderNode._refresh_instance_state()`
reports loopback for both address lists.

The stress threads then copy the files into each stress container at run time —
`stress_thread.py`, `scylla_bench_thread.py`, `cql_stress_cassandra_stress_thread.py` and
`latte_thread.py` all iterate `loader.ssl_conf_dir` and `send_files()` each file into
`/etc/scylla/ssl_conf/`. That transfer goes through `RemoteDocker.send_files()` (mktemp +
`docker cp`), not through `DockerCmdRunner`.

## File transfer: tar streams instead of SCP

`DockerCmdRunner` has no SSH connection. `send_files()` builds a tar archive in memory and hands
it to `container.put_archive()`; `receive_files()` does the reverse with `get_archive()`. The
archive member names decide where files land, so `_create_tar_stream()` implements rsync-like
trailing-slash semantics:

| `src` | Archive members | Result under `dst` |
|---|---|---|
| `/host/ssl_conf/1.2.3.4/` | `ca.pem`, `db.crt`, … | contents copied into `dst` |
| `/host/ssl_conf/1.2.3.4` | `1.2.3.4/ca.pem`, … | the directory itself copied into `dst` |
| `/host/f.txt` → `dst=/tmp/g.txt` | `g.txt` | renamed on arrival |
| `/host/f.txt` → `dst=/tmp/` | `f.txt` | name preserved |

The relative base is the source directory itself when `src` ends with `/`, and its parent
otherwise. Getting this wrong is what originally produced a nested
`/tmp/ssl_conf/<ip>/<ip>/ca.pem` and left Scylla with no certificate at the configured path.

`send_files()` also has to pick the directory to extract into, since `put_archive()` takes a
target path rather than a full destination:

```python
extraction_dir = dst if dst.endswith("/") or not dst_path.suffix else str(dst_path.parent)
```

A `dst` with a trailing slash, or no filename suffix, is treated as a directory; anything else is
treated as a file path and the archive is unpacked into its parent. **`Path.suffix` is misleading
for IP-named directories** — `Path("/tmp/ssl_conf/10.0.0.5").suffix` is `".5"`, so that path is
classified as a file and the archive is extracted into `/tmp/ssl_conf`. Always pass a trailing
slash when the destination is a directory whose last component could look like it has an
extension.

That is exactly why `install_client_certificate()` spells its destinations out:

```python
dst = "/tmp/ssl_conf/"
remoter.run(f"mkdir -p {dst}")
remoter.send_files(src=str(Path(get_data_dir_path("ssl_conf")) / node_identifier) + "/", dst=dst)
remoter.run(f"mkdir -p {dst}client/")
remoter.send_files(src=str(Path(get_data_dir_path("ssl_conf")) / "client") + "/", dst=dst + "client/")
```

Both source and destination carry trailing slashes, and each destination subdirectory is created
before the transfer — `put_archive()` fails if the target path does not exist. The function
finishes by moving `/tmp/ssl_conf` to `/etc/scylla/ssl_conf` and dropping `cqlshrc` into
`~/.cassandra/` and `/root/.cassandra/`.

The trailing-slash rules are locked down by `unit_tests/unit/test_docker_tar_stream.py`.

## Troubleshooting

**`install_client_certificate()` returns immediately and certificates are stale.**
It short-circuits when `/etc/scylla/ssl_conf` already exists unless `force=True` is passed. A
container reused across runs keeps the old directory. The Docker `node_setup()` path passes
`force=True`; ad-hoc calls may not.

**Scylla starts without TLS even though `server_encrypt: true`.**
Check `internode_encryption` — without it, `server_encryption_options` is omitted from
`scylla.yaml`. Verify on the node with
`grep -A5 '^server_encryption_options:' /etc/scylla/scylla.yaml`.

**Files landed one directory too deep (`/etc/scylla/ssl_conf/<ip>/ca.pem`).**
A `send_files()` call passed a source directory without a trailing slash. Compare against the
table above.

**`put_archive()` fails with "no such directory".**
The extraction directory did not exist. `send_files()` runs `mkdir -p` on the computed
`extraction_dir`, but if `dst` was misclassified as a file (the `Path.suffix` trap) the wrong
directory is created. Add the trailing slash.

**cassandra-stress fails on the truststore.**
The JKS is built by a `java` container on the SCT host during `create_ca()`. If the host cannot
pull that image, `truststore.jks` is missing and `_generate_loader_certs()` fails when copying it.

**Hostname verification failures.**
With `peer_verification: true`, the stress tool matches the certificate against the address it
connects to. Docker node certificates carry the container name and the bridge IP as SANs — a
connection made over any other address will not match.

**Inspecting what actually reached a node.**
`docker exec <container> ls -la /etc/scylla/ssl_conf/` shows the installed material; the host-side
originals live in `data_dir/ssl_conf/<node-ip>/` until teardown removes them.

## Related

- [Docker backend specifics](docker-backend-overview.md)
- [Moving to docker based loaders](docker-loaders.md)
- `sdcm/provision/helpers/certificate.py` — certificate generation and installation
- `sdcm/remote/docker_cmd_runner.py` — tar-stream file transfer
- `sdcm/cluster_docker.py` — `ScyllaDockerCluster` and `LoaderSetDocker` node setup
