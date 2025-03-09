import logging
import json
import sys
import os

from typing import Any
from pathlib import Path

from sdcm.es import ES
sys.path.append(os.path.join(os.path.dirname(__file__), "..", ".."))


LOGGER = logging.getLogger(__name__)


def create_index(index_name: str, mappings: dict, **kwargs):
    if not mappings:
        raise Exception(f"Mappings configuration is empty {mappings}")
    es_client = ES()
    es_client.indices.create(index=index_name)
    es_client.indices.put_mapping(index=index_name, body=mappings,  # pylint: disable=unexpected-keyword-arg
                                  include_type_name=True)


def get_mapping(mapping_filepath: str) -> Any:
    file_content = Path(mapping_filepath).read_text(encoding="utf-8")
    mapping = json.loads(file_content)
    return mapping
