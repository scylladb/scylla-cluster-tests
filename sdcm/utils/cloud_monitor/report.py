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
# Copyright (c) 2021 ScyllaDB
import abc
import os
import tempfile
from abc import abstractmethod

from datetime import datetime, timedelta
from collections import defaultdict
from copy import deepcopy

import jinja2
import pytz

from sdcm.keystore import KeyStore
from sdcm.utils.cloud_monitor.resources import CLOUD_PROVIDERS
from sdcm.utils.cloud_monitor.resources.capacity_reservations import CapacityReservation
from sdcm.utils.cloud_monitor.resources.instances import CloudInstances
from sdcm.utils.cloud_monitor.resources.static_ips import StaticIPs


class BaseReport:

    def __init__(self, cloud_instances: CloudInstances, static_ips: StaticIPs | None, html_template: str):
        self.cloud_instances = cloud_instances
        self.static_ips = static_ips
        self.html_template = html_template

    @property
    def templates_dir(self):
        cur_path = os.path.dirname(os.path.abspath(__file__))
        return os.path.join(cur_path, "templates")

    def _jinja_render_template(self, **kwargs):
        loader = jinja2.FileSystemLoader(self.templates_dir)
        env = jinja2.Environment(loader=loader, autoescape=True, extensions=['jinja2.ext.loopcontrols'],
                                 finalize=lambda x: x if x != 0 else "")
        template = env.get_template(self.html_template)
        html = template.render(**kwargs)
        return html

    def render_template(self):
        return self._jinja_render_template(**vars(self))

    def to_html(self):
        return self.render_template()

    def to_file(self):
        with tempfile.NamedTemporaryFile(mode="w", encoding="utf-8",
                                         prefix='cloud-report_', delete=False, suffix='.html') as report_file:
            report_file.write(self.to_html())
            return report_file.name


class CloudResourcesReport(BaseReport):
    def __init__(self, cloud_instances: CloudInstances, static_ips: StaticIPs):
        super().__init__(cloud_instances, static_ips, html_template="cloud_resources.html")
        stats = dict(num_running_instances=0,
                     num_stopped_instances=0,
                     unused_static_ips=0,
                     num_used_static_ips=0,
                     )
        self.report = {cloud_provider: deepcopy(stats) for cloud_provider in CLOUD_PROVIDERS}

    def to_html(self):
        for cloud_provider in CLOUD_PROVIDERS:
            num_running_instances = len([i for i in self.cloud_instances[cloud_provider] if i.state == "running"])
            num_stopped_instances = len([i for i in self.cloud_instances[cloud_provider] if i.state == "stopped"])
            num_unused_static_ips = len([i for i in self.static_ips[cloud_provider] if not i.is_used])
            self.report[cloud_provider]["num_running_instances"] = num_running_instances
            self.report[cloud_provider]["num_stopped_instances"] = num_stopped_instances
            self.report[cloud_provider]["num_unused_static_ips"] = num_unused_static_ips
            self.report[cloud_provider]["num_used_static_ips"] = len(self.static_ips[cloud_provider])
        return self.render_template()


class PerUserSummaryReport(BaseReport):
    def __init__(self, cloud_instances: CloudInstances, static_ips: StaticIPs, crs: list[CapacityReservation]):
        super().__init__(cloud_instances, static_ips, html_template="per_user_summary.html")
        self.report = {"results": {"qa": {}, "others": {}}, "cloud_providers": CLOUD_PROVIDERS, "crs": crs}
        self.qa_users = KeyStore().get_qa_users()

    def user_type(self, user_name: str):
        return "qa" if user_name in self.qa_users else "others"

    def to_html(self):
        for cloud_provider in CLOUD_PROVIDERS:
            for instance in self.cloud_instances[cloud_provider]:
                user_type = self.user_type(instance.owner)
                results = self.report["results"]
                if instance.owner not in results[user_type]:
                    stats = dict(num_running_instances_spot=0, num_running_instances_on_demand=0,
                                 num_stopped_instances=0)
                    results[user_type][instance.owner] = {cp: deepcopy(stats) for cp in self.report["cloud_providers"]}
                    results[user_type][instance.owner]["num_instances_keep_alive"] = 0
                    results[user_type][instance.owner]["total_cost"] = 0
                    results[user_type][instance.owner]["projected_daily_cost"] = 0
                if instance.state == "running":
                    if instance.lifecycle == "spot":
                        results[user_type][instance.owner][cloud_provider]["num_running_instances_spot"] += 1
                    else:
                        results[user_type][instance.owner][cloud_provider]["num_running_instances_on_demand"] += 1
                    results[user_type][instance.owner]["total_cost"] += instance.total_cost
                    results[user_type][instance.owner]["projected_daily_cost"] += instance.projected_daily_cost
                if instance.state == "stopped":
                    results[user_type][instance.owner][cloud_provider]["num_stopped_instances"] += 1
                if instance.keep:
                    results[user_type][instance.owner]["num_instances_keep_alive"] += 1
        return self.render_template()


class GeneralReport(BaseReport):
    def __init__(self, cloud_instances: CloudInstances, static_ips: StaticIPs, crs: list[CapacityReservation]):
        super().__init__(cloud_instances, static_ips, html_template="base.html")
        self.cloud_resources_report = CloudResourcesReport(cloud_instances=cloud_instances, static_ips=static_ips)
        self.per_user_report = PerUserSummaryReport(cloud_instances, static_ips, crs)

    def to_html(self):
        cloud_resources_html = self.cloud_resources_report.to_html()
        per_user_report_html = self.per_user_report.to_html()
        return self._jinja_render_template(body=cloud_resources_html + per_user_report_html)


class DetailedReport(BaseReport):
    """Attached as HTML file"""

    def __init__(self, cloud_instances: CloudInstances, static_ips: StaticIPs, user=None):
        super().__init__(cloud_instances, static_ips, html_template="per_user.html")
        self.user = user
        self.report = defaultdict(list)

    def to_html(self):
        for instance in self.cloud_instances.all:
            self.report[instance.owner].append(instance)
        if self.user:
            self.report = {self.user: self.report.get(self.user)}
        resources_html = self.render_template()
        self.html_template = "base.html"
        return self._jinja_render_template(body=resources_html)


class InstancesTimeDistributionReport(BaseReport, metaclass=abc.ABCMeta):
    def __init__(self, cloud_instances: CloudInstances, user=None):
        super().__init__(cloud_instances, static_ips=None, html_template="per_qa_user.html")
        self.user = user
        self.report = {"unknown": defaultdict(list), "7": defaultdict(
            list), "5": defaultdict(list), "3": defaultdict(list)}
        self.qa_users = KeyStore().get_qa_users()

    def to_html(self):
        for instance in self.cloud_instances.all:
            if self._is_user_be_skipped(instance) or self._is_instance_be_skipped(instance):
                continue
            if not instance.create_time:
                self.report["unknown"][instance.owner].append(instance)
            elif self._is_older_than_3days(instance.create_time):
                self.report["3"][instance.owner].append(instance)
            elif self._is_older_than_5days(instance.create_time):
                self.report["5"][instance.owner].append(instance)
            elif self._is_older_than_7days(instance.create_time):
                self.report["7"][instance.owner].append(instance)
            else:
                continue
        if self.user:
            for key in self.report:
                self.report[key] = {self.user: self.report[key].get(self.user, [])}

        resources_html = self.render_template()
        self.html_template = "base.html"
        return self._jinja_render_template(body=resources_html)

    @staticmethod
    def _is_older_than_3days(create_time):
        return pytz.utc.localize(datetime.utcnow() - timedelta(days=3)) > create_time > pytz.utc.localize(
            datetime.utcnow() - timedelta(days=5))

    @staticmethod
    def _is_older_than_5days(create_time):
        return pytz.utc.localize(datetime.utcnow() - timedelta(days=5)) > create_time > pytz.utc.localize(
            datetime.utcnow() - timedelta(days=7))

    @staticmethod
    def _is_older_than_7days(create_time):
        return create_time < pytz.utc.localize(datetime.utcnow() - timedelta(days=7))

    @abstractmethod
    def _is_user_be_skipped(self, instance):
        ...

    @staticmethod
    def _is_instance_be_skipped(instance):
        return instance.state != "running"


class QAonlyInstancesTimeDistributionReport(InstancesTimeDistributionReport):
    def _is_user_be_skipped(self, instance):
        return instance.owner == "qa" or instance.owner not in self.qa_users


class NonQaInstancesTimeDistributionReport(InstancesTimeDistributionReport):
    def _is_user_be_skipped(self, instance):
        return instance.owner == "qa" or instance.owner in self.qa_users
