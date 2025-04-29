import logging
from collections import defaultdict
from pathlib import Path
from typing import NamedTuple

import yaml

from sdcm.utils.metaclasses import Singleton

LOGGER = logging.getLogger(__name__)


class MapKeywords(NamedTuple):
    keyword: str
    issue: str


class FindIssuePerBacktrace(metaclass=Singleton):

    MAPPING_FILE = Path(__file__).parent / "issue_by_keyword.yaml"

    def __init__(self):
        with open(file=self.MAPPING_FILE, encoding="utf-8", mode="r") as map_file:
            yaml_details = yaml.safe_load(map_file)

        self.map = defaultdict(list)
        for backtrace_type, values in yaml_details.items():
            for one_pair in values:
                self.map[backtrace_type].append(MapKeywords(keyword=one_pair["keyword"], issue=one_pair["issue"]))

    def find_issue(self, backtrace_type: str, decoded_backtrace: str) -> str | None:
        LOGGER.debug("Search for an issue for %s backtrace", backtrace_type)
        for all_keywords in self.map.get(backtrace_type, []):
            if all_keywords.keyword in decoded_backtrace:
                return all_keywords.issue
        return None
