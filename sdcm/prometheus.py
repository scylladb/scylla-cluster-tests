import socket
import logging
import threading
try:
    from BaseHTTPServer import HTTPServer
    from SocketServer import ThreadingMixIn
except ImportError:
    # Python 3
    from http.server import HTTPServer
    from socketserver import ThreadingMixIn

import prometheus_client

START = 'start'
STOP = 'stop'

LOGGER = logging.getLogger(__name__)
NM_OBJ = None


class _ThreadingSimpleServer(ThreadingMixIn, HTTPServer):
    """Thread per request HTTP server."""


def start_http_server(port, addr='', registry=prometheus_client.REGISTRY):
    """Starts an HTTP server for prometheus metrics as a daemon thread"""
    custom_metrics_handler = prometheus_client.MetricsHandler.factory(registry)
    httpd = _ThreadingSimpleServer((addr, port), custom_metrics_handler)
    http_thread = threading.Thread(target=httpd.serve_forever)
    http_thread.daemon = True
    http_thread.start()
    return httpd


def start_metrics_server():
    """
    https://github.com/prometheus/prometheus/wiki/Default-port-allocations
    Occupied port 9389 for SCT
    """
    hostname = socket.gethostname()

    try:
        LOGGER.debug('Try to start prometheus API server')
        httpd = start_http_server(0)
        port = httpd.server_port
        ip = socket.gethostbyname(hostname)

        LOGGER.info('prometheus API server running on port: %s', port)
        return '{}:{}'.format(ip, port)
    except Exception as ex:  # pylint: disable=broad-except
        LOGGER.error('Cannot start local http metrics server: %s', ex)

    return None


def nemesis_metrics_obj():
    global NM_OBJ  # pylint: disable=global-statement
    if not NM_OBJ:
        NM_OBJ = NemesisMetrics()
    return NM_OBJ


class NemesisMetrics(object):

    DISRUPT_COUNTER = 'nemesis_disruptions_counter'
    DISRUPT_GAUGE = 'nemesis_disruptions_gauge'

    def __init__(self):
        super(NemesisMetrics, self).__init__()
        self._disrupt_counter = self.create_counter(self.DISRUPT_COUNTER,
                                                    'Counter for nemesis disruption methods',
                                                    ['method', 'event'])
        self._disrupt_gauge = self.create_gauge(self.DISRUPT_GAUGE,
                                                'Gauge for nemesis disruption methods',
                                                ['method'])

    @staticmethod
    def create_counter(name, desc, param_list):
        try:
            return prometheus_client.Counter(name, desc, param_list)
        except Exception as ex:  # pylint: disable=broad-except
            LOGGER.error('Cannot create metrics counter: %s', ex)
        return None

    @staticmethod
    def create_gauge(name, desc, param_list):
        try:
            return prometheus_client.Gauge(name, desc, param_list)
        except Exception as ex:  # pylint: disable=broad-except
            LOGGER.error('Cannot create metrics gauge: %s', ex)
        return None

    def event_start(self, disrupt):
        try:
            self._disrupt_counter.labels(disrupt, START).inc()  # pylint: disable=no-member
            self._disrupt_gauge.labels(disrupt).inc()  # pylint: disable=no-member
        except Exception as ex:  # pylint: disable=broad-except
            LOGGER.exception('Cannot start metrics event: %s', ex)

    def event_stop(self, disrupt):
        try:
            self._disrupt_counter.labels(disrupt, STOP).inc()  # pylint: disable=no-member
            self._disrupt_gauge.labels(disrupt).dec()  # pylint: disable=no-member
        except Exception as ex:  # pylint: disable=broad-except
            LOGGER.exception('Cannot stop metrics event: %s', ex)
