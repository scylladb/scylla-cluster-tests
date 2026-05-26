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
import os
import re
from dataclasses import replace as dc_replace
from pathlib import Path

import click
import yaml

_SSD_SUFFIX_RE = re.compile(r"-(\d+)ssd$")


def _display_name(inst) -> str:
    """Return display name: strip synthetic -Nssd suffix for GCE n2-highmem variants."""
    return _SSD_SUFFIX_RE.sub("", inst.instance_type)


def _disk_str(inst) -> str:
    """Format disk display: show disk count when >1, e.g. '6000 (16d)'."""
    if not inst.local_disk_gb:
        return "0"
    total = f"{inst.local_disk_gb:.0f}"
    if inst.local_disk_count > 1:
        return f"{total} ({inst.local_disk_count}d)"
    return total


@click.group("sizing", help="Instance sizing tools: browse catalog, resolve constraints, preview configs")
def sizing_group():
    pass


@sizing_group.command("catalog", help="Browse instance catalog with pricing for a cloud provider")
@click.option("--cloud", type=click.Choice(["aws", "gce", "azure", "oci"]), required=True, help="Cloud provider")
@click.option(
    "--role",
    type=click.Choice(["db", "loader", "monitor"]),
    default=None,
    help="Filter by role (shows preferred families)",
)
@click.option("--family", type=str, default=None, help="Filter by instance family")
@click.pass_context
def sizing_catalog(ctx, cloud, role, family):
    # lazy: sdcm imports pull boto3/google-cloud/azure-mgmt — too heavy for CLI startup
    from sdcm.utils.cloud_catalog.instance_catalog import InstanceCatalog  # noqa: PLC0415

    catalog_dir = Path(__file__).parent / "data" / "instance_catalog"
    try:
        catalog = InstanceCatalog.from_directory(catalog_dir)
    except Exception as exc:  # noqa: BLE001
        click.echo(f"Error loading catalog from {catalog_dir}: {exc}", err=True)
        ctx.exit(1)
        return

    instances = catalog.get_instances(cloud)

    if family:
        instances = [i for i in instances if i.family == family]

    if not instances:
        click.echo(f"No instances found for cloud={cloud}" + (f", family={family}" if family else ""))
        return

    preferred_families: list[str] = []
    if role:
        preferred_families = catalog.preferred_families.get(role, {}).get(cloud, [])

    family_rank: dict[str, int] = {fam: idx for idx, fam in enumerate(preferred_families)}

    instances = sorted(instances, key=lambda i: (family_rank.get(i.family, len(preferred_families)), i.vcpus))

    header = f"{'Instance Type':<25}  {'vCPUs':>5}  {'Memory(GB)':>10}  {'Disk(GB)':>8}  {'Arch':<8}  {'Price/hr':>8}"
    separator = f"{'─' * 25}  {'─' * 5}  {'─' * 10}  {'─' * 8}  {'─' * 8}  {'─' * 8}"
    click.echo(header)
    click.echo(separator)
    for inst in instances:
        price_str = f"${inst.price_per_hour:.4f}" if inst.price_per_hour is not None else "N/A"
        click.echo(
            f"{_display_name(inst):<25}  {inst.vcpus:>5}  {inst.memory_gb:>10.2f}  "
            f"{_disk_str(inst):>8}  {inst.arch:<8}  {price_str:>8}"
        )


@sizing_group.command("update-catalog", help="Refresh instance catalog from cloud provider APIs")
@click.option(
    "--cloud",
    type=click.Choice(["aws", "gce", "azure", "oci", "all"]),
    default="all",
    help="Cloud to update",
)
@click.pass_context
def sizing_update_catalog(ctx, cloud):
    # lazy: sdcm imports pull boto3/google-cloud/azure-mgmt — too heavy for CLI startup
    from sdcm.utils.cloud_catalog.catalog_generator import update_catalog  # noqa: PLC0415

    output_dir = Path(__file__).parent / "data" / "instance_catalog"
    cloud_arg = None if cloud == "all" else cloud
    clouds_label = cloud if cloud != "all" else "all clouds"
    click.echo(f"Updating instance catalog for {clouds_label} → {output_dir}")
    try:
        update_catalog(cloud=cloud_arg, output_dir=output_dir)
        click.echo("Catalog update complete.")
    except Exception as exc:  # noqa: BLE001
        click.echo(f"Error updating catalog: {exc}", err=True)
        ctx.exit(1)


@sizing_group.command("resolve", help="Resolve hardware constraints to instance types across all clouds")
@click.option("--vcpu", type=str, required=True, help="vCPU constraint (e.g. 8, 8-16, >=32, <64)")
@click.option("--memory", type=str, default=None, help="Memory constraint (e.g. 32, >16gb)")
@click.option("--disk", type=str, default=None, help="Disk constraint")
@click.option("--arch", type=str, default=None, help="Architecture (arm64, x86_64, arm, x86)")
@click.option("--role", type=click.Choice(["db", "loader", "monitor"]), default="db", help="Role for family preference")
@click.option("--region", type=str, default=None, help="Region for pricing (e.g. us-east-1, eu-west-1)")
@click.pass_context
def sizing_resolve(ctx, vcpu, memory, disk, arch, role, region):
    # lazy: sdcm imports pull boto3/google-cloud/azure-mgmt — too heavy for CLI startup
    from sdcm.utils.cloud_catalog.instance_catalog import InstanceCatalog  # noqa: PLC0415
    from sdcm.utils.cloud_catalog.instance_matcher import select_instance, NoMatchingInstanceError  # noqa: PLC0415

    catalog_dir = Path(__file__).parent / "data" / "instance_catalog"
    try:
        catalog = InstanceCatalog.from_directory(catalog_dir)
    except Exception as exc:  # noqa: BLE001
        click.echo(f"Error loading catalog from {catalog_dir}: {exc}", err=True)
        ctx.exit(1)
        return

    constraints: dict = {"vcpu": vcpu}
    if memory:
        constraints["memory"] = memory
    if disk:
        constraints["disk"] = disk
    if arch:
        constraints["arch"] = arch

    clouds = ["aws", "gce", "azure", "oci"]

    header = f"{'Cloud':<8}  {'Resolved Instance':<25}  {'vCPUs':>5}  {'Memory(GB)':>10}  {'Disk(GB)':>8}  {'Arch':<8}  {'Price/hr':>8}"
    separator = f"{'─' * 8}  {'─' * 25}  {'─' * 5}  {'─' * 10}  {'─' * 8}  {'─' * 8}  {'─' * 8}"
    click.echo(header)
    click.echo(separator)

    for cloud_name in clouds:
        try:
            inst = select_instance(
                catalog=catalog, role=role, cloud=cloud_name, constraints=constraints, arch=arch, region=region
            )
            price = inst.get_price(region)
            price_str = f"${price:.4f}" if price is not None else "N/A"
            disk_str = _disk_str(inst)
            click.echo(
                f"{cloud_name:<8}  {_display_name(inst):<25}  {inst.vcpus:>5}  {inst.memory_gb:>10.2f}  {disk_str:>8}  {inst.arch:<8}  {price_str:>8}"
            )
        except NoMatchingInstanceError:
            click.echo(
                f"{cloud_name:<8}  {click.style('No match', fg='red'):<34}  {'':>5}  {'':>10}  {'':>8}  {'':>8}  {'':>8}"
            )
        except Exception as exc:  # noqa: BLE001
            click.echo(f"{cloud_name:<8}  Error: {exc}")


def _resolve_config_files(config_files: tuple[str, ...]) -> list[str]:
    # lazy: pulls groovy parser dependencies not needed unless Jenkinsfiles are passed
    from sdcm.utils.lint.jenkins_parser import parse_jenkinsfile  # noqa: PLC0415

    resolved = []
    for path_str in config_files:
        path = Path(path_str)
        if path.suffix in {".jenkinsfile", ".groovy"} or path.name == "Jenkinsfile":
            pipeline_config = parse_jenkinsfile(path)
            if pipeline_config and pipeline_config.test_config:
                resolved.extend(pipeline_config.test_config)
            else:
                click.echo(f"Warning: no test_config found in {path}", err=True)
        else:
            resolved.append(path_str)
    return resolved


@sizing_group.command("preview", help="Preview how a config file resolves instance types across all clouds")
@click.argument("config_files", nargs=-1, required=True, type=click.Path(exists=True))
@click.option(
    "--sizing-db", type=str, default=None, help="Override db sizing constraints, e.g. 'vcpu=32,memory=128,arch=x86_64'"
)
@click.option(
    "--sizing-loader", type=str, default=None, help="Override loader sizing constraints, e.g. 'vcpu=8,memory=16'"
)
@click.option(
    "--sizing-monitor", type=str, default=None, help="Override monitor sizing constraints, e.g. 'vcpu=4,memory=16'"
)
@click.option(
    "--instance-type-db", type=str, default=None, help="Override AWS db instance type (literal), e.g. 'i3en.2xlarge'"
)
@click.option("--instance-type-loader", type=str, default=None, help="Override AWS loader instance type (literal)")
@click.option("--instance-type-monitor", type=str, default=None, help="Override AWS monitor instance type (literal)")
@click.option("--duration", type=int, default=None, help="Override test duration in minutes (for cost calculation)")
@click.pass_context
def sizing_preview(  # noqa: PLR0912, PLR0914, PLR0915
    ctx,
    config_files,
    sizing_db,
    sizing_loader,
    sizing_monitor,
    instance_type_db,
    instance_type_loader,
    instance_type_monitor,
    duration,
):
    # lazy: sdcm imports pull boto3/google-cloud/azure-mgmt — too heavy for CLI startup
    from sdcm.utils.cloud_catalog.instance_catalog import InstanceCatalog  # noqa: PLC0415
    from sdcm.utils.cloud_catalog.instance_matcher import (  # noqa: PLC0415
        is_literal_instance_type,
        select_instance,
        NoMatchingInstanceError,
    )

    config_files = _resolve_config_files(config_files)
    if not config_files:
        click.echo("Error: no config files found (Jenkinsfile had no test_config?)", err=True)
        ctx.exit(1)
        return

    catalog_dir = Path(__file__).parent / "data" / "instance_catalog"
    try:
        catalog = InstanceCatalog.from_directory(catalog_dir)
    except Exception as exc:  # noqa: BLE001
        click.echo(f"Error loading catalog from {catalog_dir}: {exc}", err=True)
        ctx.exit(1)
        return

    defaults_dir = Path(__file__).parent / "defaults"
    default_configs: dict = {}
    for defaults_file in sorted(defaults_dir.glob("*.yaml")):
        if defaults_file.stem.startswith("k8s_"):
            continue
        with open(defaults_file) as fh:
            data = yaml.safe_load(fh)
            if data:
                default_configs.update(data)

    merged_config: dict = {}
    merged_config.update(default_configs)
    user_config: dict = {}
    for config_file in config_files:
        with open(config_file) as fh:
            data = yaml.safe_load(fh)
            if data:
                user_config.update(data)
                merged_config.update(data)

    env_overrides: dict = {}
    for key, val in os.environ.items():
        if not key.startswith("SCT_"):
            continue
        base_key = key[4:].lower()
        dot_pos = base_key.find(".")
        if dot_pos != -1:
            param_name = base_key[:dot_pos]
            sub_key = base_key[dot_pos + 1 :]
            env_overrides.setdefault(param_name, {})
            if isinstance(env_overrides[param_name], dict):
                env_overrides[param_name][sub_key] = val.strip()
        else:
            stripped = val.strip()
            try:
                parsed = yaml.safe_load(stripped)
            except Exception:  # noqa: BLE001
                parsed = stripped
            env_overrides[base_key] = parsed
    merged_config.update(env_overrides)

    def _parse_constraints(raw: str) -> dict:
        result = {}
        for part in raw.split(","):
            k, _, v = part.partition("=")
            k, v = k.strip(), v.strip()
            if k:
                result[k] = v
        return result

    cli_overrides: dict = {}
    if sizing_db:
        cli_overrides["sizing_db"] = _parse_constraints(sizing_db)
    if sizing_loader:
        cli_overrides["sizing_loader"] = _parse_constraints(sizing_loader)
    if sizing_monitor:
        cli_overrides["sizing_monitor"] = _parse_constraints(sizing_monitor)
    if instance_type_db:
        cli_overrides["instance_type_db"] = instance_type_db
    if instance_type_loader:
        cli_overrides["instance_type_loader"] = instance_type_loader
    if instance_type_monitor:
        cli_overrides["instance_type_monitor"] = instance_type_monitor
    if duration is not None:
        cli_overrides["test_duration"] = duration
    merged_config.update(cli_overrides)

    CLOUD_PARAM = {
        "aws": {
            "db": "instance_type_db",
            "db_oracle": "instance_type_db_oracle",
            "zero_token": "zero_token_instance_type_db",
            "loader": "instance_type_loader",
            "monitor": "instance_type_monitor",
        },
        "gce": {
            "db": "gce_instance_type_db",
            "loader": "gce_instance_type_loader",
            "monitor": "gce_instance_type_monitor",
        },
        "azure": {
            "db": "azure_instance_type_db",
            "db_oracle": "azure_instance_type_db_oracle",
            "loader": "azure_instance_type_loader",
            "monitor": "azure_instance_type_monitor",
        },
        "oci": {
            "db": "oci_instance_type_db",
            "db_oracle": "oci_instance_type_db_oracle",
            "loader": "oci_instance_type_loader",
            "monitor": "oci_instance_type_monitor",
        },
    }

    NODE_COUNT_PARAMS = {
        "db": "n_db_nodes",
        "db_oracle": "n_test_oracle_db_nodes",
        "zero_token": "n_db_zero_token_nodes",
        "loader": "n_loaders",
        "monitor": "n_monitor_nodes",
    }

    CLOUD_REGION_PARAM = {
        "aws": "region_name",
        "gce": "gce_datacenter",
        "azure": "azure_region_name",
        "oci": "oci_region_name",
    }

    clouds = ["aws", "gce", "azure", "oci"]
    roles = ["db", "db_oracle", "zero_token", "loader", "monitor"]

    cloud_regions: dict[str, str | None] = {}
    for cloud_name in clouds:
        raw = merged_config.get(CLOUD_REGION_PARAM.get(cloud_name, ""))
        if isinstance(raw, list):
            cloud_regions[cloud_name] = raw[0] if raw else None
        elif isinstance(raw, str) and raw:
            cloud_regions[cloud_name] = raw
        else:
            cloud_regions[cloud_name] = None

    test_duration_min = merged_config.get("test_duration", 0)
    test_duration_hr = test_duration_min / 60.0 if test_duration_min else 0

    click.echo(f"Config files: {', '.join(config_files)}")
    if test_duration_min:
        click.echo(f"Test duration: {test_duration_min} min ({test_duration_hr:.1f} hr)")
    click.echo()

    def _derive_constraints(inst_info):
        constraints = {"vcpu": f">={inst_info.vcpus}"}
        if inst_info.memory_gb:
            constraints["memory"] = f">={inst_info.memory_gb}"
        if inst_info.local_disk_gb:
            constraints["disk"] = f">={inst_info.local_disk_gb}"
        return constraints

    def _find_literal(value, cloud_name):
        instances = catalog.get_instances(cloud_name)
        match = next((i for i in instances if i.instance_type == value), None)
        if match:
            return match
        parts = value.split(":")
        if len(parts) == 3:
            base_type = f"{parts[0]}:{parts[1]}"
            base_match = next((i for i in instances if i.instance_type == base_type), None)
            if base_match:
                mem_gb = float(parts[2])
                return dc_replace(base_match, instance_type=value, memory_gb=mem_gb)
        return None

    def _format_row(cloud_name, inst, source, node_count):
        region = cloud_regions.get(cloud_name)
        price = inst.get_price(region)
        price_str = f"${price:.4f}" if price is not None else "N/A"
        disk_str = _disk_str(inst)
        cost_str = ""
        if price is not None and test_duration_hr and node_count:
            cost = price * test_duration_hr * node_count
            cost_str = f"${cost:.2f}"
        return (
            f"  {cloud_name:<8}  {_display_name(inst):<25}  {inst.vcpus:>5}  "
            f"{inst.memory_gb:>10.2f}  {disk_str:>8}  {inst.arch:<8}  {price_str:>8}  "
            f"{cost_str:>10}  {source}"
        )

    total_cost_by_cloud: dict[str, float] = {c: 0.0 for c in clouds}
    has_no_match: dict[str, bool] = {c: False for c in clouds}

    AGNOSTIC_PARAMS = {
        "db": "sizing_db",
        "db_oracle": "sizing_db_oracle",
        "zero_token": "sizing_db",
        "loader": "sizing_loader",
        "monitor": "sizing_monitor",
    }

    for role in roles:
        role_values: dict[str, object] = {}
        role_sources: dict[str, str] = {}
        agnostic_param = AGNOSTIC_PARAMS[role]
        agnostic_value = merged_config.get(agnostic_param)
        for cloud_name in clouds:
            param_name = CLOUD_PARAM[cloud_name].get(role)
            if not param_name:
                continue
            if param_name in cli_overrides and cli_overrides[param_name]:
                role_values[cloud_name] = cli_overrides[param_name]
                role_sources[cloud_name] = (param_name, "cli")
            elif agnostic_param in cli_overrides and cli_overrides[agnostic_param]:
                role_values[cloud_name] = cli_overrides[agnostic_param]
                role_sources[cloud_name] = (agnostic_param, "cli")
            elif param_name in env_overrides and env_overrides[param_name]:
                role_values[cloud_name] = env_overrides[param_name]
                role_sources[cloud_name] = (param_name, "env-var")
            elif agnostic_param in env_overrides and env_overrides[agnostic_param]:
                role_values[cloud_name] = env_overrides[agnostic_param]
                role_sources[cloud_name] = (agnostic_param, "env-var")
            elif param_name in user_config and user_config[param_name]:
                role_values[cloud_name] = user_config[param_name]
                role_sources[cloud_name] = (param_name, "test-config")
            elif agnostic_param in user_config and isinstance(user_config.get(agnostic_param), dict):
                role_values[cloud_name] = user_config[agnostic_param]
                role_sources[cloud_name] = (agnostic_param, "test-config")
            elif isinstance(agnostic_value, dict) and param_name != agnostic_param:
                role_values[cloud_name] = agnostic_value
                role_sources[cloud_name] = (agnostic_param, "defaults")
            elif param_name in merged_config and merged_config[param_name]:
                role_values[cloud_name] = merged_config[param_name]
                role_sources[cloud_name] = (param_name, "defaults")

        aws_param = CLOUD_PARAM["aws"].get(role)
        default_value = merged_config.get(aws_param) if aws_param else None
        if not role_values and not default_value:
            continue

        node_count_raw = str(merged_config.get(NODE_COUNT_PARAMS.get(role, ""), 0))
        node_count = sum(int(x) for x in node_count_raw.split())
        if node_count == 0:
            continue
        if role == "db_oracle" and merged_config.get("db_type") not in ("mixed_scylla", "mixed_cassandra"):
            continue
        click.echo(f"Role: {role} (× {node_count} nodes)")
        click.echo()

        header = (
            f"  {'Cloud':<8}  {'Resolved Instance':<25}  {'vCPUs':>5}  {'Memory(GB)':>10}  "
            f"{'Disk(GB)':>8}  {'Arch':<8}  {'Price/hr':>8}  {'Est. Cost':>10}  {'Source'}"
        )
        separator = (
            f"  {'─' * 8}  {'─' * 25}  {'─' * 5}  {'─' * 10}  {'─' * 8}  {'─' * 8}  {'─' * 8}  {'─' * 10}  {'─' * 20}"
        )
        click.echo(header)
        click.echo(separator)

        reference_inst = None

        for cloud_name in clouds:
            value = role_values.get(cloud_name, default_value)
            if not value:
                click.echo(
                    f"  {cloud_name:<8}  {'N/A':<25}  {'':>5}  {'':>10}  {'':>8}  {'':>8}  {'':>8}  {'':>10}  no config"
                )
                continue

            param_name = CLOUD_PARAM[cloud_name].get(role)
            if not param_name:
                click.echo(
                    f"  {cloud_name:<8}  {'N/A':<25}  {'':>5}  {'':>10}  {'':>8}  {'':>8}  {'':>8}  {'':>10}  no config"
                )
                continue
            if cloud_name in role_values:
                src_key, src_origin = role_sources.get(cloud_name, (param_name, "defaults"))
                source_param = f"{src_key} ({src_origin})"
            else:
                source_param = f"{param_name} (defaults)"

            if isinstance(value, str) and is_literal_instance_type(value):
                match = _find_literal(value, cloud_name)
                if match:
                    click.echo(_format_row(cloud_name, match, f"{source_param} (literal)", node_count))
                    match_price = match.get_price(cloud_regions.get(cloud_name))
                    if match_price is not None and test_duration_hr and node_count:
                        total_cost_by_cloud[cloud_name] += match_price * test_duration_hr * node_count
                    if reference_inst is None:
                        reference_inst = match
                else:
                    cross_match = None
                    for other_cloud in clouds:
                        if other_cloud == cloud_name:
                            continue
                        cross_match = _find_literal(value, other_cloud)
                        if cross_match:
                            break
                    if cross_match:
                        constraints = _derive_constraints(cross_match)
                        if reference_inst is None:
                            reference_inst = cross_match
                        try:
                            inst = select_instance(
                                catalog=catalog,
                                role=role,
                                cloud=cloud_name,
                                constraints=constraints,
                                arch=cross_match.arch,
                                region=cloud_regions.get(cloud_name),
                            )
                            click.echo(
                                _format_row(
                                    cloud_name,
                                    inst,
                                    f"≈ {value} → {constraints} (estimated)",
                                    node_count,
                                )
                            )
                            inst_price = inst.get_price(cloud_regions.get(cloud_name))
                            if inst_price is not None and test_duration_hr and node_count:
                                total_cost_by_cloud[cloud_name] += inst_price * test_duration_hr * node_count
                        except NoMatchingInstanceError:
                            has_no_match[cloud_name] = True
                            click.echo(
                                f"  {cloud_name:<8}  {click.style('No match', fg='red'):<34}  {'':>5}  {'':>10}  "
                                f"{'':>8}  {'':>8}  {'':>8}  {'':>10}  ≈ {value} (no equivalent)"
                            )
                    else:
                        click.echo(
                            f"  {cloud_name:<8}  {value:<25}  {'?':>5}  {'?':>10}  "
                            f"{'?':>8}  {'?':<8}  {'?':>8}  {'':>10}  {source_param} (not in catalog)"
                        )
            elif isinstance(value, dict):
                try:
                    inst = select_instance(
                        catalog=catalog,
                        role=role,
                        cloud=cloud_name,
                        constraints=value,
                        region=cloud_regions.get(cloud_name),
                    )
                    click.echo(_format_row(cloud_name, inst, f"{source_param} (resolved)", node_count))
                    inst_price = inst.get_price(cloud_regions.get(cloud_name))
                    if inst_price is not None and test_duration_hr and node_count:
                        total_cost_by_cloud[cloud_name] += inst_price * test_duration_hr * node_count
                except NoMatchingInstanceError:
                    has_no_match[cloud_name] = True
                    click.echo(
                        f"  {cloud_name:<8}  {click.style('No match', fg='red'):<34}  {'':>5}  {'':>10}  "
                        f"{'':>8}  {'':>8}  {'':>8}  {'':>10}  {source_param}"
                    )
                except Exception as exc:  # noqa: BLE001
                    click.echo(f"  {cloud_name:<8}  Error: {exc}")
            else:
                click.echo(
                    f"  {cloud_name:<8}  {'N/A':<25}  {'':>5}  {'':>10}  {'':>8}  {'':>8}  {'':>8}  {'':>10}  no config"
                )

        click.echo()

    if test_duration_hr and any(v > 0 for v in total_cost_by_cloud.values()):
        click.echo("Estimated Total Cost")
        click.echo(f"  {'Cloud':<8}  {'Total':>10}")
        click.echo(f"  {'─' * 8}  {'─' * 10}")
        for cloud_name in clouds:
            if has_no_match[cloud_name]:
                continue
            cost = total_cost_by_cloud[cloud_name]
            if cost > 0:
                click.echo(f"  {cloud_name:<8}  ${cost:>9.2f}")
        click.echo()
