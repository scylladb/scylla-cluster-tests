#!/usr/bin/python3
import sys
import dnslib.server
import dnslib
import random
import _thread
import urllib.request
import time
import socket

from dnslib import DNSRecord, RCODE

# The list of live nodes, all of them supposedly answering HTTP requests on
# alternator_port. One of these nodes will be returned at random from every
# DNS request. This list starts with one or more known nodes, but then the
# livenodes_update() thread periodically replaces this list by an up-to-date
# list retrieved from makeing a "localnodes" requests to one of these nodes.
livenodes = [sys.argv[1]]
alternator_port = sys.argv[2]


def livenodes_update():
    global alternator_port  # noqa: PLW0602
    global livenodes  # noqa: PLW0603
    while True:
        # Contact one of the already known nodes by random, to fetch a new
        # list of known nodes.
        ip = random.choice(livenodes)
        url = 'http://{}:{}/localnodes'.format(ip, alternator_port)
        print('updating livenodes from {}'.format(url))
        try:
            nodes = urllib.request.urlopen(url, None, 1.0).read().decode('ascii')
            a = [x.strip('"').rstrip('"') for x in nodes.strip('[').rstrip(']').split(',')]
            # If we're successful, replace livenodes by the new list
            livenodes = a
            print(livenodes)
        except Exception:  # noqa: BLE001
            # TODO: contacting this ip was unsuccessful, maybe we should
            # remove it from the list of live nodes.
            pass
        time.sleep(1)


_thread.start_new_thread(livenodes_update, ())


class Resolver:
    def __init__(self):
        self.address = '8.8.8.8'
        self.port = 53
        self.timeout = 5

    def resolve(self, request, handler):
        qname = request.q.qname
        reply = request.reply()
        # Note responses have TTL 4, as in Amazon's Dynamo DNS
        print(qname)

        if qname == 'alternator':
            ip = random.choice(livenodes)
            reply.add_answer(*dnslib.RR.fromZone('{} 4 A {}'.format(qname, ip)))

        # Otherwise proxy
        if not reply.rr:
            try:
                if handler.protocol == 'udp':
                    proxy_r = request.send(self.address, self.port,
                                           timeout=self.timeout)
                else:
                    proxy_r = request.send(self.address, self.port,
                                           tcp=True, timeout=self.timeout)
                reply = DNSRecord.parse(proxy_r)
            except socket.timeout:
                reply.header.rcode = getattr(RCODE, 'NXDOMAIN')

        return reply


resolver = Resolver()
logger = dnslib.server.DNSLogger(prefix=True)
tcp_server = dnslib.server.DNSServer(Resolver(), port=53, address='0.0.0.0', logger=logger, tcp=True)
tcp_server.start_thread()

udp_server = dnslib.server.DNSServer(Resolver(), port=53, address='0.0.0.0', logger=logger, tcp=False)
udp_server.start_thread()

try:
    while True:
        time.sleep(10)
except KeyboardInterrupt:
    print('Goodbye!')
finally:
    tcp_server.stop()
    udp_server.stop()
