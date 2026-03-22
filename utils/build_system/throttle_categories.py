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
# Copyright (c) 2026 ScyllaDB

"""Manage Jenkins Throttle Concurrent Builds plugin categories.

The throttle-concurrents plugin has no REST API for category management.
Categories are stored in the global Jenkins config and can only be managed
via Groovy scripts executed through the Jenkins Script Console API.

Usage::

    from utils.build_system.throttle_categories import ThrottleCategoryManager

    manager = ThrottleCategoryManager()
    manager.ensure_category("SCT-perf-us-east-1", max_total=3, max_per_node=1)
    manager.ensure_categories([
        ThrottleCategory("SCT-perf-us-east-1", max_total=3, max_per_node=1),
        ThrottleCategory("SCT-perf-eu-west-1", max_total=2, max_per_node=1),
    ])
"""

import json
import logging
from dataclasses import dataclass, field

import jenkins

from sdcm.keystore import KeyStore

LOGGER = logging.getLogger(__name__)

JENKINS_URL = "https://jenkins.scylladb.com"

# Groovy script that returns all existing throttle categories as JSON.
GROOVY_GET_CATEGORIES = """\
import groovy.json.JsonOutput
import hudson.plugins.throttleconcurrents.ThrottleJobProperty

def descriptor = Jenkins.instance.getDescriptorByType(ThrottleJobProperty.DescriptorImpl)
def result = descriptor.categories.collect { cat ->
    [
        categoryName: cat.categoryName,
        maxConcurrentPerNode: cat.maxConcurrentPerNode,
        maxConcurrentTotal: cat.maxConcurrentTotal,
        nodeLabeledPairs: cat.nodeLabeledPairs.collect { pair ->
            [throttledNodeLabel: pair.throttledNodeLabel, maxConcurrentPerNodeLabeled: pair.maxConcurrentPerNodeLabeled]
        }
    ]
}
println JsonOutput.toJson(result)
"""

# Groovy script template that replaces all categories with the provided JSON list.
# Expects a placeholder {categories_json} containing a JSON array of category objects.
GROOVY_SET_CATEGORIES = """\
import groovy.json.JsonSlurper
import hudson.plugins.throttleconcurrents.ThrottleJobProperty

def descriptor = Jenkins.instance.getDescriptorByType(ThrottleJobProperty.DescriptorImpl)
def jsonData = new JsonSlurper().parseText('''{categories_json}''')

def newCategories = jsonData.collect {{ item ->
    def pairs = item.nodeLabeledPairs.collect {{ pair ->
        new ThrottleJobProperty.NodeLabeledPair(pair.throttledNodeLabel, pair.maxConcurrentPerNodeLabeled)
    }}
    new ThrottleJobProperty.ThrottleCategory(
        item.categoryName,
        item.maxConcurrentPerNode,
        item.maxConcurrentTotal,
        pairs
    )
}}

descriptor.setCategories(newCategories)
descriptor.save()
println "OK: " + newCategories.size() + " categories saved"
"""


@dataclass
class NodeLabeledPair:
    """A node-label-specific concurrency limit within a throttle category."""

    throttled_node_label: str
    max_concurrent_per_node_labeled: int = 0


@dataclass
class ThrottleCategory:
    """A throttle-concurrents plugin category definition.

    Args:
        name: Category name referenced in pipeline ``throttle([name])`` blocks.
        max_total: Maximum concurrent builds across all nodes (0 = unlimited).
        max_per_node: Maximum concurrent builds per node (0 = unlimited).
        node_labeled_pairs: Optional per-label concurrency limits.
    """

    name: str
    max_total: int = 0
    max_per_node: int = 0
    node_labeled_pairs: list[NodeLabeledPair] = field(default_factory=list)

    def to_dict(self) -> dict:
        return {
            "categoryName": self.name,
            "maxConcurrentTotal": self.max_total,
            "maxConcurrentPerNode": self.max_per_node,
            "nodeLabeledPairs": [
                {
                    "throttledNodeLabel": p.throttled_node_label,
                    "maxConcurrentPerNodeLabeled": p.max_concurrent_per_node_labeled,
                }
                for p in self.node_labeled_pairs
            ],
        }

    @classmethod
    def from_dict(cls, data: dict) -> "ThrottleCategory":
        return cls(
            name=data["categoryName"],
            max_total=data.get("maxConcurrentTotal", 0),
            max_per_node=data.get("maxConcurrentPerNode", 0),
            node_labeled_pairs=[
                NodeLabeledPair(
                    throttled_node_label=p["throttledNodeLabel"],
                    max_concurrent_per_node_labeled=p.get("maxConcurrentPerNodeLabeled", 0),
                )
                for p in data.get("nodeLabeledPairs", [])
            ],
        )


class ThrottleCategoryManager:
    """Manage throttle-concurrents categories on a Jenkins instance.

    Args:
        jenkins_url: Jenkins server URL.
        username: Jenkins username. If None, reads from KeyStore.
        password: Jenkins API token/password. If None, reads from KeyStore.
    """

    def __init__(self, jenkins_url: str = JENKINS_URL, username: str | None = None, password: str | None = None):
        if not username and not password:
            creds = KeyStore().get_json("jenkins.json")
            username, password = creds.get("username"), creds.get("password")
        self.server = jenkins.Jenkins(jenkins_url, username=username, password=password)

    def _run_script(self, script: str) -> str:
        """Execute a Groovy script on Jenkins and return stripped output."""
        result = self.server.run_script(script)
        return result.strip() if result else ""

    def get_categories(self) -> list[ThrottleCategory]:
        """Fetch all existing throttle categories from Jenkins.

        Returns:
            List of ThrottleCategory objects currently configured.
        """
        output = self._run_script(GROOVY_GET_CATEGORIES)
        if not output:
            return []
        data = json.loads(output)
        return [ThrottleCategory.from_dict(item) for item in data]

    def _save_categories(self, categories: list[ThrottleCategory]) -> None:
        """Replace all throttle categories on Jenkins with the given list."""
        categories_json = json.dumps([c.to_dict() for c in categories])
        script = GROOVY_SET_CATEGORIES.format(categories_json=categories_json)
        output = self._run_script(script)
        LOGGER.info("Throttle categories saved: %s", output)

    def ensure_category(
        self,
        name: str,
        max_total: int = 0,
        max_per_node: int = 0,
        node_labeled_pairs: list[NodeLabeledPair] | None = None,
    ) -> bool:
        """Ensure a throttle category exists with the specified settings.

        If a category with the given name already exists and has different settings,
        it is updated. If it already matches, no change is made.

        Args:
            name: Category name.
            max_total: Maximum concurrent builds total (0 = unlimited).
            max_per_node: Maximum concurrent builds per node (0 = unlimited).
            node_labeled_pairs: Optional per-label concurrency limits.

        Returns:
            True if a change was made, False if category already matched.
        """
        desired = ThrottleCategory(
            name=name,
            max_total=max_total,
            max_per_node=max_per_node,
            node_labeled_pairs=node_labeled_pairs or [],
        )
        categories = self.get_categories()
        existing = {c.name: c for c in categories}

        if name in existing and existing[name].to_dict() == desired.to_dict():
            LOGGER.info("Category '%s' already exists with matching settings, skipping", name)
            return False

        if name in existing:
            LOGGER.info("Updating category '%s'", name)
            categories = [desired if c.name == name else c for c in categories]
        else:
            LOGGER.info("Creating category '%s'", name)
            categories.append(desired)

        self._save_categories(categories)
        return True

    def ensure_categories(self, desired_categories: list[ThrottleCategory]) -> list[str]:
        """Ensure multiple throttle categories exist, creating or updating as needed.

        Performs a single read and a single write to minimize Jenkins API calls.
        Existing categories not in the desired list are preserved unchanged.

        Args:
            desired_categories: List of ThrottleCategory objects to ensure.

        Returns:
            List of category names that were created or updated.
        """
        current = self.get_categories()
        existing_map = {c.name: c for c in current}
        changed = []

        for desired in desired_categories:
            if desired.name in existing_map:
                if existing_map[desired.name].to_dict() != desired.to_dict():
                    existing_map[desired.name] = desired
                    changed.append(desired.name)
                    LOGGER.info("Will update category '%s'", desired.name)
            else:
                existing_map[desired.name] = desired
                changed.append(desired.name)
                LOGGER.info("Will create category '%s'", desired.name)

        if changed:
            self._save_categories(list(existing_map.values()))
            LOGGER.info("Created/updated %d categories: %s", len(changed), changed)
        else:
            LOGGER.info("All %d categories already up to date", len(desired_categories))

        return changed

    def delete_category(self, name: str) -> bool:
        """Delete a throttle category by name.

        Args:
            name: Category name to delete.

        Returns:
            True if the category was found and deleted, False if not found.
        """
        categories = self.get_categories()
        filtered = [c for c in categories if c.name != name]
        if len(filtered) == len(categories):
            LOGGER.warning("Category '%s' not found, nothing to delete", name)
            return False

        self._save_categories(filtered)
        LOGGER.info("Deleted category '%s'", name)
        return True

    def list_categories(self) -> list[str]:
        """Return names of all configured throttle categories."""
        return [c.name for c in self.get_categories()]
