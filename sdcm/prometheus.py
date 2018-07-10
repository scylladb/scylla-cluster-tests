import socket
import logging
import prometheus_client

from avocado.utils import network

START = 'start'
STOP = 'stop'
ERROR = 'error'

logger = logging.getLogger(__name__)
nm_obj = None


def start_metrics_server(port=9389):
    """
    https://github.com/prometheus/prometheus/wiki/Default-port-allocations
    Occupied port 9389 for SCT
    """
    hostname = socket.gethostname()
    if not network.is_port_free(port, hostname):
        port = network.find_free_port(8001, 10000)

    try:
        logger.debug('Try to start prometheus API server on port: %s', port)
        prometheus_client.start_http_server(port)
        ip = socket.gethostbyname(hostname)
        return '{}:{}'.format(ip, port)
    except Exception as ex:
        logger.error('Cannot start local http metrics server: %s', ex)

    return None


def sct_metrics_obj():
    global nm_obj
    if not nm_obj:
        nm_obj = SCTMetrics()
    return nm_obj


def log_error(method):
    def wrapper(*args, **kwargs):
        try:
            return method(*args, **kwargs)
        except Exception as ex:
            logger.exception('Prometheus error: %s: %s', method.__name__, ex)
    return wrapper


class SCTMetrics(object):

    DISRUPT_COUNTER = 'nemesis_disruptions_counter'
    DISRUPT_GAUGE = 'nemesis_disruptions_gauge'
    DISRUPT_ERROR_COUNTER = 'nemesis_disruption_error_counter'
    COREDUMP_COUNTER = 'coredump_counter'
    DATABASE_ERROR_COUNTER = 'database_error_counter'
    CRITICAL_ERROR_COUNTER = 'critical_error_counter'

    def __init__(self):
        super(SCTMetrics, self).__init__()
        self._disrupt_counter = self._create_counter(self.DISRUPT_COUNTER,
                                                     'Counter for nemesis disruption methods',
                                                     ['method', 'event'])
        self._disrupt_gauge = self._create_gauge(self.DISRUPT_GAUGE,
                                                 'Gauge for nemesis disruption methods',
                                                 ['method'])
        self._disrupt_error_counter = self._create_counter(self.DISRUPT_ERROR_COUNTER,
                                                           'Counter for nemesis disruption errors',
                                                           ['method', 'event'])
        self._coredump_counter = self._create_counter(self.COREDUMP_COUNTER,
                                                      'Counter for coredumps',
                                                      ['node', 'event'])
        self._database_error_counter = self._create_counter(self.DATABASE_ERROR_COUNTER,
                                                            'Counter for database errors',
                                                            ['error', 'event'])
        self._critical_error_counter = self._create_counter(self.CRITICAL_ERROR_COUNTER,
                                                            'Counter for critical errors',
                                                            ['error', 'event'])

    @staticmethod
    def _create_counter(name, desc, param_list):
        try:
            return prometheus_client.Counter(name, desc, param_list)
        except Exception as ex:
            logger.error('Cannot create metrics counter: %s', ex)
        return None

    @staticmethod
    def _create_gauge(name, desc, param_list):
        try:
            return prometheus_client.Gauge(name, desc, param_list)
        except Exception as ex:
            logger.error('Cannot create metrics gauge: %s', ex)
        return None

    @log_error
    def event_start(self, disrupt):
        self._disrupt_counter.labels(disrupt, START).inc()
        self._disrupt_gauge.labels(disrupt).inc()

    @log_error
    def event_stop(self, disrupt):
        self._disrupt_counter.labels(disrupt, STOP).inc()
        self._disrupt_gauge.labels(disrupt).dec()

    @log_error
    def nemesis_error_event(self, disrupt):
        self._disrupt_error_counter.labels(disrupt, ERROR).inc()

    @log_error
    def coredump_event(self, node):
        self._coredump_counter.labels(node, ERROR).inc()

    @log_error
    def database_error_event(self, error):
        self._database_error_counter.labels(error, ERROR).inc()

    @log_error
    def critical_error_event(self, error):
        self._critical_error_counter.labels(error, ERROR).inc()
