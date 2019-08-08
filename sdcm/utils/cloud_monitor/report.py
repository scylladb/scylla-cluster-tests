import os
from collections import defaultdict

import jinja2
import tempfile


class BaseReport(object):

    def __init__(self, cloud_instances, html_template):
        self.cloud_instances = cloud_instances
        self.html_template = html_template

    @property
    def templates_dir(self):
        cur_path = os.path.dirname(os.path.abspath(__file__))
        return os.path.join(cur_path, "templates")

    def _jinja_render_template(self, **kwargs):
        loader = jinja2.FileSystemLoader(self.templates_dir)
        env = jinja2.Environment(loader=loader, autoescape=True, extensions=['jinja2.ext.loopcontrols'])
        template = env.get_template(self.html_template)
        html = template.render(**kwargs)
        return html

    def render_template(self):
        return self._jinja_render_template(**vars(self))

    def to_html(self):
        return self.render_template()

    def to_file(self):
        with tempfile.NamedTemporaryFile(prefix='cloud-report_', delete=False, suffix='.html') as report_file:
            report_file.write(self.to_html())
            return report_file.name


class CloudResourcesReport(BaseReport):
    def __init__(self, cloud_instances):
        super(CloudResourcesReport, self).__init__(cloud_instances, html_template="cloud_resources.html")
        stats = dict(num_running_instances=0,
                     num_stopped_instances=0,
                     )
        self.report = {cloud: dict(stats) for cloud in cloud_instances.instances.keys()}

    def to_html(self):
        for cloud in self.cloud_instances.instances:
            num_running_instances = len([i for i in self.cloud_instances[cloud] if i.state == "running"])
            num_stopped_instances = len([i for i in self.cloud_instances[cloud] if i.state == "stopped"])
            self.report[cloud]["num_running_instances"] = num_running_instances
            self.report[cloud]["num_stopped_instances"] = num_stopped_instances
        return self.render_template()


class PerUserSummaryReport(BaseReport):
    def __init__(self, cloud_instances):
        super(PerUserSummaryReport, self).__init__(cloud_instances, html_template="per_user_summary.html")
        self.stats = dict(num_running_instances=0, num_stopped_instances=0)
        self.report = {}

    def to_html(self):
        self.report = {}
        for cloud in self.cloud_instances.instances:
            for instance in self.cloud_instances[cloud]:
                if instance.owner not in self.report:
                    self.report[instance.owner] = dict(self.stats)
                if instance.state == "running":
                    self.report[instance.owner]["num_running_instances"] += 1
                if instance.state == "stopped":
                    self.report[instance.owner]["num_stopped_instances"] += 1
        return self.render_template()


class GeneralReport(BaseReport):
    def __init__(self, cloud_instances):
        super(GeneralReport, self).__init__(cloud_instances, html_template="base.html")
        self.cloud_resources_report = CloudResourcesReport(cloud_instances)
        self.per_user_report = PerUserSummaryReport(cloud_instances)

    def to_html(self):
        cloud_resources_html = self.cloud_resources_report.to_html()
        per_user_report_html = self.per_user_report.to_html()
        return self._jinja_render_template(body=cloud_resources_html + per_user_report_html)


class DetailedReport(BaseReport):
    def __init__(self, cloud_instances, user=None):
        super(DetailedReport, self).__init__(cloud_instances, html_template="per_user.html")
        self.user = user
        self.report = defaultdict(list)

    def to_html(self):
        for instance in self.cloud_instances.all_instances:
            self.report[instance.owner].append(instance)
        if self.user:
            self.report = {self.user: self.report.get(self.user)}
        resources_html = self.render_template()
        self.html_template = "base.html"
        return self._jinja_render_template(body=resources_html)
