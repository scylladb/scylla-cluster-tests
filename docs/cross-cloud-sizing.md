# Cross-Cloud Instance Sizing

SCT supports abstract T-shirt instance sizes that automatically resolve to cloud-specific instance types based on the active backend. This abstraction enables cloud-agnostic test configurations and easier cross-cloud comparisons.

## Overview

Historically, SCT test configurations required specifying exact instance types for each cloud backend. As SCT expanded to support AWS, GCE, Azure, and OCI, this led to verbose configuration files.

Cross-cloud sizing introduces canonical size names (like `2xlarge`) that map to equivalent hardware specifications across all supported providers.

## Usage

Instead of specifying multiple cloud-specific parameters:

```yaml
# Before: Multiple lines for different clouds
instance_type_db: 'i4i.2xlarge'
gce_instance_type_db: 'z3-highmem-16'
gce_n_local_ssd_disk_db: 4
azure_instance_type_db: 'Standard_L16s_v4'
```

You can use a single abstract size:

```yaml
# After: Single line resolves for all clouds
db_instance_type: '2xlarge'
```

### Available Parameters

The following parameters support abstract sizing:

- `db_instance_type`: Abstract size for database nodes.
- `loader_instance_type`: Abstract size for loader nodes.
- `monitor_instance_type`: Abstract size for monitoring nodes.

## Resolution Rules

1. **Precedence**: If a cloud-specific parameter (e.g., `instance_type_db`, `gce_instance_type_db`) is explicitly set in the configuration, it takes precedence over the abstract size.
2. **Resolution**: If an abstract size (e.g., `db_instance_type`) is set, it resolves to the cloud-specific parameters for the active backend.
3. **Fallback**: If neither is set, the system falls back to the backend-specific defaults defined in the framework.
4. **AWS Architecture**: On AWS, abstract sizes resolve to `i8g` (ARM/Graviton4) by default. To use x86 (`i4i`), set `aws_architecture: x86`.

## Size Mapping Table

### DB Role
Storage-optimized instances with local NVMe SSDs.

| Size | AWS ARM | AWS x86 | GCE | Azure | OCI |
| :--- | :--- | :--- | :--- | :--- | :--- |
| **large** | i8g.large | i4i.large | z3-highmem-4 | Standard_L4s_v4 | DenseIO.E5.Flex:2:32 |
| **xlarge** | i8g.xlarge | i4i.xlarge | z3-highmem-8 | Standard_L8s_v4 | DenseIO.E5.Flex:4:64 |
| **2xlarge** | i8g.2xlarge | i4i.2xlarge | z3-highmem-16 | Standard_L16s_v4 | DenseIO.E5.Flex:8:128 |
| **4xlarge** | i8g.4xlarge | i4i.4xlarge | z3-highmem-32 | Standard_L32s_v4 | DenseIO.E5.Flex:16:256 |
| **8xlarge** | i8g.8xlarge | i4i.8xlarge | z3-highmem-48 | Standard_L64s_v4 | DenseIO.E5.Flex:32:512 |
| **16xlarge** | i8g.16xlarge | i4i.16xlarge | z3-highmem-88 | Standard_L80s_v4 | DenseIO.E5.Flex:64:1024 |

### Loader Role
Compute-optimized instances for stress tools.

| Size | AWS (x86/ARM) | GCE | Azure | OCI |
| :--- | :--- | :--- | :--- | :--- |
| **small** | c6i.xlarge | e2-standard-4 | Standard_F4s_v2 | VM.Standard3.Flex:4:32 |
| **medium** | c6i.2xlarge | e2-standard-8 | Standard_F8s_v2 | VM.Standard3.Flex:8:64 |
| **large** | c6i.4xlarge | e2-standard-16 | Standard_F16s_v2 | VM.Standard3.Flex:16:128 |
| **xlarge** | c6i.16xlarge | e2-highcpu-32 | Standard_F32s_v2 | VM.Standard3.Flex:32:256 |

### Monitor Role
General-purpose instances for the monitoring stack.

| Size | AWS (x86/ARM) | GCE | Azure | OCI |
| :--- | :--- | :--- | :--- | :--- |
| **small** | t3.large | n2-highmem-4 | Standard_D2_v4 | VM.Standard.E4.Flex:2:16 |
| **medium** | m6i.xlarge | n2-highmem-8 | Standard_D4_v4 | VM.Standard.E4.Flex:4:32 |
| **large** | m6i.2xlarge | n2-highmem-16 | Standard_D8_v4 | VM.Standard.E4.Flex:8:64 |

## CLI Tools

The SCT CLI provides utilities to inspect and test size mappings.

### show-sizes
Displays the full mapping table for a given role.
```bash
uv run sct.py show-sizes --role db
```

### translate-size
Shows how an abstract size resolves for a specific cloud.
```bash
uv run sct.py translate-size --role db --size 2xlarge --cloud gce
```

## Adding New Sizes

To add a new size or modify existing mappings, edit `sdcm/utils/cloud_sizes.py`.

1. Add the new size to the `SIZING` dictionary under the appropriate role.
2. Define a `SizeMapping` with `InstanceSpec` for each cloud.
3. If the instance requires special parameters (like GCE local SSDs), include them in the `InstanceSpec`.
