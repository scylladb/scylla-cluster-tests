from sdcm.utils.ci_tools import get_test_name

# pylint: disable=too-few-public-methods


class Dashboard:
    name: str
    path: str
    title: str


class OverviewDashboard(Dashboard):
    name = 'overview'
    path = 'd/overview-{version}/scylla-{dashboard_name}'
    title = 'Overview'


class ServerMetricsNemesisDashboard(Dashboard):
    if test_name := get_test_name():
        test_name = f"{test_name.lower()}-"

    name = f'{test_name}scylla-per-server-metrics-nemesis'
    title = 'Scylla Per Server Metrics Nemesis'
    path = 'dashboard/db/{dashboard_name}-{version}'


class AlternatorDashboard(Dashboard):
    name = 'alternator'
    title = 'Alternator'
    path = 'd/alternator-{version}/{dashboard_name}'
