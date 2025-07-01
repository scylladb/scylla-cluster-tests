import re
import time
import logging

from typing import Dict, Union, List
from random import choice, randint

LOGGER = logging.getLogger(__name__)

CDC_LOGTABLE_SUFFIX = "_scylla_cdc_log"
CDC_SETTINGS_NAMES_VALUES = {
    "enabled": [True, False],
    "delta": ["full", "keys"],
    "preimage": ["full", True, False],
    "postimage": [True, False],
    "ttl": "86400",
}

CDC_DELTA_REGEXP = r"delta.*?(?P<delta>(full|keys))"
CDC_ENABLED_REGEXP = r"enabled.*?(?P<enabled>(true|false))"
CDC_PREIMAGE_REGEXP = r"preimage.*?(?P<preimage>(full|false|true))"
CDC_POSTIMAGE_REGEXP = r"postimage.*?(?P<postimage>(true|false))"
CDC_TTL_REGEXP = r"ttl.*?(?P<ttl>\d+)"

CDC_SETTINGS_REGEXP = [CDC_ENABLED_REGEXP, CDC_DELTA_REGEXP, CDC_PREIMAGE_REGEXP, CDC_POSTIMAGE_REGEXP, CDC_TTL_REGEXP]


def parse_cdc_blob_settings(blob: bytes) -> Dict[str, Union[bool, str]]:
    """parse blob object with cdc setttings

    cdc settings stored in MetaTableData as blob.
    b'\x05\x00\x00\x00\x05\x00\x00\x00delta\x04\x00\x00\x00full
      \x07\x00\x00\x00enabled\x04\x00\x00\x00true
      \t\x00\x00\x00postimage\x05\x00\x00\x00true
      \x08\x00\x00\x00preimage\x05\x00\x00\x00false
      \x03\x00\x00\x00ttl\x05\x00\x00\x00860'

    using regexp the bytes string parsed to dict
        cdc_settings = {"delta": "full",
                    "enabled": "true",
                    "preimage": "false",
                    "postimage": "true",
                    "ttl": "860"}

    :param blob: blob object containing cdc settings
    :type blob: bytes
    :returns: dict with cdc settings
    :rtype: {dict}
    """
    cdc_settings = {"delta": "full", "enabled": False, "preimage": False, "postimage": False, "ttl": "86400"}

    for regexp in CDC_SETTINGS_REGEXP:
        res = re.search(regexp, blob.decode())
        if res:
            for key, value in res.groupdict().items():
                if value in ("false", "off"):
                    value = False
                elif value == "true":
                    value = True

                cdc_settings[key] = value

    return cdc_settings


def get_table_cdc_properties(session, ks_name: str, table_name: str) -> Dict[str, Union[bool, str]]:
    """Return cdc settings for table

    Get cdc settings from table meta data and return dict
    cdc_settings = {"delta": "full",
                    "enabled": "true",
                    "preimage": "false",
                    "postimage": "true",
                    "ttl": "860"}

    :param ks_name: [description]
    :type ks_name: str
    :param table_name: [description]
    :type table_name: str
    :returns: [description]
    :rtype: {Dict[str, str]}
    """

    session.cluster.refresh_schema_metadata()
    # For some reason, `refresh_schema_metadata` doesn't refresh immediatelly...
    time.sleep(10)
    ks = session.cluster.metadata.keyspaces[ks_name]
    raw_cdc_extention = ks.tables[table_name].extensions["cdc"]
    LOGGER.debug("Blob object with cdc settings: %s", raw_cdc_extention)
    cdc_settings = parse_cdc_blob_settings(raw_cdc_extention)
    LOGGER.debug("CDC settings as dict: %s", cdc_settings)

    return cdc_settings


def toggle_cdc_property(name: str, value: Union[str, bool]) -> Union[bool, str]:
    if name in ["ttl"]:
        return str(randint(300, 3600))
    else:
        variants = set(CDC_SETTINGS_NAMES_VALUES[name]) - {value}
        return choice(list(variants))


def get_cdc_settings_names() -> List[str]:
    return list(CDC_SETTINGS_NAMES_VALUES.keys())
