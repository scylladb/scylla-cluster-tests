from abc import ABC
from dataclasses import dataclass, asdict, field, fields

from sdcm.provision.provisioner import InstanceDefinition

#  pylint: disable=invalid-name, too-many-instance-attributes


@dataclass(frozen=True)
class GCEDiskTypes:
    SCRATCH: str = "SCRATCH"
    PERSISTENT: str = "PERSISTENT"


@dataclass(frozen=True)
class GCEInitializeParamsDiskTypes:
    PD_SSD: str = "pd-ssd"
    LOCAL_SSD: str = "local-ssd"


@dataclass(frozen=True)
class GCEDiskInterfaces:
    NVME: str = "NVME"


@dataclass
class DiskStructArgs:
    instance_definition: InstanceDefinition
    disk_type: str
    gce_services_project_name: str
    location_name: str
    local_disk_count: int
    persistent_disks: dict


@dataclass
class GCEDiskStruct(ABC):
    _instance_definition: InstanceDefinition
    _gce_services_project_name: str
    _location_name: str
    _disk_type: str
    type: str
    deviceName: str = field(init=False)
    initializeParams: dict = field(init=False)
    autoDelete: bool

    def as_struct(self) -> dict:
        private_fields = [item.name for item in fields(self) if item.name.startswith("_")]
        struct = asdict(self)

        for private_field in private_fields:
            struct.pop(private_field)

        return struct

    @staticmethod
    def _get_disk_url(name: str, region_name: str, disk_type: str) -> str:
        return f"projects/{name}/zones/{region_name}/diskTypes/{disk_type}"


@dataclass
class GCERootDiskStruct(GCEDiskStruct):
    boot: bool

    def __post_init__(self):
        self.deviceName = f"{self._instance_definition.name}-root-{self._disk_type}"
        self.initializeParams = {
            "diskType": self._get_disk_url(name=self._gce_services_project_name,
                                           region_name=self._location_name,
                                           disk_type=self._disk_type),
            "diskSizeGb": self._instance_definition.root_disk_size,
            "sourceImage": self._instance_definition.image_id
        }


@dataclass
class GCELocalDiskStruct(GCEDiskStruct):
    _index: int
    interface: str

    def __post_init__(self):
        self.deviceName = f"{self._instance_definition.name}-data-local-ssd-{self._index}"
        self.initializeParams = {
            "diskType": self._get_disk_url(name=self._gce_services_project_name,
                                           region_name=self._location_name,
                                           disk_type=self._disk_type),
        }


@dataclass
class GCEPersistentDiskStruct(GCEDiskStruct):
    _disk_size: int

    def __post_init__(self):
        self.deviceName = f"{self._instance_definition.name}-data-{self._disk_type}"
        self.initializeParams = {
            "diskType": self._get_disk_url(name=self._gce_services_project_name,
                                           region_name=self._location_name,
                                           disk_type=self._disk_type),
            "diskSizeGb": self._disk_size,
        }


# pylint:disable=too-few-public-methods
class DiskStructProvider:
    @staticmethod
    def get_disks_struct(disk_struct_args: DiskStructArgs, params: dict):
        disk_structs = [GCERootDiskStruct(
            _disk_type=disk_struct_args.disk_type,
            _gce_services_project_name=disk_struct_args.gce_services_project_name,
            _location_name=disk_struct_args.location_name,
            _instance_definition=disk_struct_args.instance_definition,
            type=GCEDiskTypes.PERSISTENT,
            autoDelete=params.get("gce_root_disk_auto_delete", True),
            boot=params.get("gce_root_disk_boot", True)
        ).as_struct()]

        for local_disk_index in range(disk_struct_args.local_disk_count):
            disk_structs.append(GCELocalDiskStruct(
                _instance_definition=disk_struct_args.instance_definition,
                _gce_services_project_name=disk_struct_args.gce_services_project_name,
                _location_name=disk_struct_args.location_name,
                _disk_type=GCEInitializeParamsDiskTypes.LOCAL_SSD,
                _index=local_disk_index,
                type=GCEDiskTypes.SCRATCH,
                autoDelete=params.get("gce_local_disk_auto_delete", True),
                interface=params.get("gce_local_disk_interface", GCEDiskInterfaces.NVME)
            ).as_struct())

        for persistent_disk_type, persistent_disk_size in disk_struct_args.persistent_disks.items():
            index = 0
            disk_structs.append(
                GCEPersistentDiskStruct(
                    _instance_definition=disk_struct_args.instance_definition,
                    _gce_services_project_name=disk_struct_args.gce_services_project_name,
                    _location_name=disk_struct_args.location_name,
                    _disk_type=GCEInitializeParamsDiskTypes.PD_SSD,
                    _disk_size=persistent_disk_size,
                    type=persistent_disk_type,
                    autoDelete=params.get("gce_persistent_disk_auto_delete", True)
                ).as_struct()
            )
            index += 1

        return disk_structs
