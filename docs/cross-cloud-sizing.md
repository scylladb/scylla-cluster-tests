# Cross-Cloud Instance Sizing

SCT uses a constraint-based system to specify hardware requirements. Instead of hardcoding per-cloud instance types, you define hardware constraints that the system resolves to concrete instance types for AWS, GCE, Azure, or OCI.

Literal instance type strings (like `i4i.4xlarge`) still work and bypass the resolution system for backward compatibility.

## Constraint Syntax

You can specify constraints in YAML configuration files or via environment variables.

### YAML format
Constraints are defined as a dictionary under the role's instance type parameter.

```yaml
instance_type_db:
  vcpu: 8
  memory: 32
  arch: arm64
```

### Environment variables
Use dot notation to set specific constraint fields.

```bash
export SCT_INSTANCE_TYPE_DB.VCPU=8
export SCT_INSTANCE_TYPE_DB.MEMORY=32
```

### Constraint Fields

| Field | Aliases | Type | Required | Default | Description |
|-------|---------|------|----------|---------|-------------|
| vcpu | vcpus | int or range | YES | - | Number of virtual CPUs |
| memory | memory_gb | int, float, or string | no | any | RAM in GB |
| disk | local_disk_gb | int, float, or string | no | auto for db roles | Local disk in GB |
| arch | - | string | no | per-cloud default | x86_64 or arm64 |

### Operators and Units
- **Implicit Minimum**: A plain integer (e.g., `vcpu: 8`) means `vcpu >= 8`.
- **Comparison Prefixes**: Use `>`, `>=`, `<`, `<=`.
- **Ranges**: Use a string like `"8-16"`.
- **Units**: Memory and disk default to GB. You can specify `gb` or `tb`.
- **Architecture Aliases**: `x86` maps to `x86_64`, and `arm` maps to `arm64`.
- **Default Architecture**: AWS defaults to `arm64`. Other clouds default to `x86_64`.

## Selection Algorithm

The resolver follows these steps to select the best instance:

1. **Filter**: Identify all instance types in the cloud catalog that satisfy every constraint.
2. **Implicit Local Storage**: For roles named `db`, `db_oracle`, or `zero_token`, the system requires local storage (`local_disk_count > 0`) unless a `disk` constraint is explicitly provided.
3. **Sort**:
   - Primary: Preferred family order (defined in the cloud catalog).
   - Secondary: Hourly price (ascending).
   - Tertiary: vCPU count (ascending).
4. **Select**: Return the first match from the sorted list.

If no instance matches the criteria, the system raises a `NoMatchingInstanceError` showing the constraints and the search scope.

## Examples

### Minimal (vCPU only)
```yaml
instance_type_db:
  vcpu: 8
```
On AWS, this resolves to an `i8g` instance because `arm64` is the default architecture and `i8g` is a preferred family for the `db` role.

### Multi-role Configuration
```yaml
instance_type_db:
  vcpu: 16
  memory: ">60"
instance_type_loader:
  vcpu: 8
instance_type_monitor:
  vcpu: 4
```

### Architecture Override
```yaml
instance_type_db:
  vcpu: 8
  arch: x86_64
```
This forces the use of x86 instances, such as `i4i` on AWS, instead of the `arm64` default.

### Literal Passthrough
```yaml
instance_type_db: 'i4i.4xlarge'
```
The system treats literal strings as-is and skips resolution.

## CLI Tools

All sizing commands live under the `sizing` subcommand group.

### sizing catalog
Browse the instance catalog with pricing for a cloud provider.
```bash
uv run sct.py sizing catalog --cloud aws --role db
uv run sct.py sizing catalog --cloud oci --family DenseIO.E5
```

### sizing update-catalog
Fetch the latest instance data from cloud provider APIs to refresh local catalogs.
```bash
uv run sct.py sizing update-catalog --cloud aws
uv run sct.py sizing update-catalog --cloud all
```

### sizing resolve
Resolve hardware constraints to instance types across all clouds.
```bash
uv run sct.py sizing resolve --vcpu 8 --role db
uv run sct.py sizing resolve --vcpu 8-16 --memory ">60" --role db
uv run sct.py sizing resolve --vcpu 4 --arch x86_64 --role loader
```

### sizing preview
Preview how a config file resolves instance types across all clouds.
```bash
uv run sct.py sizing preview test-cases/my-test.yaml
```

## Catalog Management

Catalog files are stored in `data/instance_catalog/{aws,gce,azure,oci}.yaml`.

Each entry in the catalog includes:
- Instance name and family
- Hardware specs: vCPUs, memory (GB), architecture
- Storage: Local disk count and total local disk size (GB)
- Price: Hourly cost

The catalog also defines `preferred_families` for different roles and `cloud_defaults` for settings like architecture. You can update these by running `update-catalog` or by editing the YAML files.

## Backends

- **AWS, GCE, Azure, OCI**: Support full constraint resolution.
- **Docker, baremetal, k8s-local-kind**: Skip resolution. The system logs a message and uses the provided values directly.
- **Kubernetes**: `k8s-eks` uses the AWS resolver, and `k8s-gke` uses the GCE resolver.

## Implementation Details

The resolution process is integrated into the `SCTConfiguration` lifecycle:

1. `_resolve_instance_sizes()` executes during `SCTConfiguration.__init__` after environment variables are merged.
2. The system checks the `cluster_backend` to select the appropriate cloud catalog.
3. For each role-based instance parameter (e.g., `instance_type_db`, `gce_instance_type_db`), the system checks the value type.
4. If the value is a dictionary, the resolver finds the best matching instance string.
5. If the value is a string, it is used as a literal.
