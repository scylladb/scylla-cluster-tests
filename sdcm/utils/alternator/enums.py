from enum import auto, Enum


class WriteIsolation(Enum):
    ALWAYS_USE_LWT = "always_use_lwt"
    FORBID_RMW = "forbid_rmw"
    ONLY_RMW_USES_LWT = "only_rmw_uses_lwt"
    UNSAFE_RMW = "unsafe_rmw"


class GeneratorModes(Enum):
    NONE = auto()
    BOOL = auto()
    NUMBER = auto()
    STRING = auto()
    BINARY = auto()
    LIST = auto()
    DICT = auto()
    MIX_TYPES = auto()


class GeneratorType(Enum):
    NUM_INT = auto()
    NUM_FLOAT = auto()
    NUM_DECIMAL = auto()
    STR_BYTE = auto()
    STR_BINARY = auto()


class YCSVSchemaTypes(Enum):
    HASH_SCHEMA = "HASH"
    HASH_AND_RANGE = "HASH_AND_RANGE"
