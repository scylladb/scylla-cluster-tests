from sdcm.utils.ci_tools import get_test_name


class Dashboard:
    name: str
    path: str
    title: str
    resolution: tuple[int]
    panel_title: str = None


class OverviewDashboard(Dashboard):
    name = 'overview'
    path = 'd/overview-{version}/scylla-{dashboard_name}'
    title = 'Overview'
    resolution = (1920, 4000)


class ServerMetricsNemesisDashboard(Dashboard):
    if test_name := get_test_name():
        test_name = f"{test_name.lower()}-"

    name = f'{test_name}scylla-per-server-metrics-nemesis'
    title = 'Scylla Per Server Metrics Nemesis'
    path = 'dashboard/db/{dashboard_name}-{version}'
    resolution = (1920, 15000)


class AlternatorDashboard(Dashboard):
    name = 'alternator'
    title = 'Alternator'
    path = 'd/alternator-{version}/{dashboard_name}'
    resolution = (1920, 4000)


class DetailedLsaTotalMemory(Dashboard):
    name = 'detailed'
    path = 'd/overview-{version}/{dashboard_name}'
    title = 'Detailed'
    panel_title = "LSA total memory"
    resolution = (1920, 4000)
