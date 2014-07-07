#!/usr/bin/python

# -*- coding: utf-8 -*-

# Copyright (C) 2009-2012:
#    Gabes Jean, naparuba@gmail.com
#    Gerhard Lausser, Gerhard.Lausser@consol.de
#    Gregory Starck, g.starck@gmail.com
#    Hartmut Goebel, h.goebel@goebel-consult.de
#
# Copyright (C) 2014 - Savoir-Faire Linux inc.
#
# This file is part of Shinken.
#
# Shinken is free software: you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# Shinken is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU Affero General Public License for more details.
#
# You should have received a copy of the GNU Affero General Public License
# along with Shinken.  If not, see <http://www.gnu.org/licenses/>.

"""
This Class is a plugin for the Shinken Broker. It is in charge
to brok information of the service/host perfdatas to Riemann.
http://riemann.io/
"""

from shinken.basemodule import BaseModule
from shinken.log import logger
from shinken.misc.perfdata import PerfDatas
from shinken.misc.logevent import LogEvent
from riemann_client.client import Client
from riemann_client.transport import TCPTransport, UDPTransport

properties = {
    'daemons': ['broker'],
    'type': 'riemann_perfdata',
    'external': False,
}


# Called by the plugin manager to get a broker
def get_instance(mod_conf):
    logger.info("[riemann broker] Get a riemann data module for plugin %s" % mod_conf.get_name())
    instance = RiemannBroker(mod_conf)
    return instance


# Class for the riemann Broker
# Get broks and send them to Riemann
class RiemannBroker(BaseModule):
    def __init__(self, modconf):
        BaseModule.__init__(self, modconf)

        self.host = getattr(modconf, 'host', 'localhost')
        self.port = int(getattr(modconf, 'port', '5555'))
        self.use_udp = getattr(modconf, 'use_udp', '0') == '1'

        self.buffer = []
        self.ticks = 0
        self.tick_limit = int(getattr(modconf, 'tick_limit', '300'))

    # Called by Broker so we can do init stuff
    # Conf from arbiter!
    def init(self):
        logger.info("[riemann broker] I init the %s server connection to %s:%d" %
                    (self.get_name(), str(self.host), self.port))

        if self.use_udp:
            transport = UDPTransport(self.host, self.port)
        else:
            transport = TCPTransport(self.host, self.port)
        transport.connect()
        self.client = Client(transport)

    # Returns a list of perfdata events
    def get_check_result_perfdata_events(self, perf_data, timestamp, host, service):

        events = []
        metrics = PerfDatas(perf_data).metrics

        for e in metrics.values():
            event_data = {
                'host': host,
                'service': service,
                'time': timestamp,
                #'state': 'WARNING',
                'metric_f': e.value,
                'description': e.name,
            }
            attributes = {}
            if e.uom is not None:
                attributes['unit'] = unicode(e.uom)

            if e.warning is not None:
                attributes['warning'] = unicode(e.warning)

            if e.critical is not None:
                attributes['critical'] = unicode(e.critical)

            if e.min is not None:
                attributes['min'] = unicode(e.min)

            if e.max is not None:
                attributes['max'] = unicode(e.max)

            if attributes is not {}:
                event_data['attributes'] = attributes

            events.append(
                self.client.create_event(event_data)
            )

        return events

    # Returns state_update points for a given check_result_brok data
    def get_state_update_points(self, data):
        events = []

        if data['state'] != data['last_state'] or \
                data['state_type'] != data['last_state_type']:

            events.append(
                self.client.create_event(
                    {
                        'host': data['host_name'],
                        'service': data['service_description'],
                        'time': data['last_chk'],
                        'attributes': {
                            'state': data['state'],
                            'state_type': data['state_type'],
                            'output': data['output']
                        }
                    }
                )
            )

        return events

    # A service check result brok has just arrived, we UPDATE data info with this
    def manage_service_check_result_brok(self, b):
        data = b.data
        events = []

        events.extend(
            self.get_check_result_perfdata_events(
                data['perf_data'],
                data['last_chk'],
                data['host_name'],
                data['service_description']
            )
        )

        events.extend(
            self.get_state_update_points(data)
        )

        try:
            logger.debug("[riemann broker] Launching: %s" % str(events))
        except UnicodeEncodeError:
            pass

        self.buffer.extend(events)

    # A host check result brok has just arrived, we UPDATE data info with this
    def manage_host_check_result_brok(self, b):
        data = b.data
        events = []

        events.extend(
            self.get_check_result_perfdata_events(
                data['perf_data'],
                data['last_chk'],
                data['host_name'],
                None,
            )
        )

        events.extend(
            self.get_state_update_points(data)
        )

        try:
            logger.debug("[riemann broker] Launching: %s" % str(events))
        except UnicodeEncodeError:
            pass

        self.buffer.extend(events)

    def manage_unknown_host_check_result_brok(self, b):
        data = b.data
        events = []

        events.extend(
            self.get_check_result_perfdata_events(
                data['perf_data'],
                data['last_chk'],
                data['host_name'],
                None,
            )
        )

        try:
            logger.debug("[riemann broker] Launching: %s" % str(events))
        except UnicodeEncodeError:
            pass

        self.buffer.extend(events)

    def manage_unknown_service_check_result_brok(self, b):
        data = b.data
        events = []

        events.extend(
            self.get_check_result_perfdata_events(
                data['perf_data'],
                data['time_stamp'],
                data['host_name'],
                data['service_description']
            )
        )

        try:
            logger.debug("[riemann broker] Launching: %s" % str(events))
        except UnicodeEncodeError:
            pass

        self.buffer.extend(events)

    # A log brok has arrived, we UPDATE data info with this
    def manage_log_brok(self, b):
        log = b.data['log']
        log_event = LogEvent(log)

        if len(log_event) > 0:

            event_data = {
                'host': b.data['hostname'],
                'service': b.data['service_desc'],
                'description': b.data['event_type'],
                'time': b.data['time']
            }

            attributes = {}

            # Add each property of the service in the point
            for prop in log_event:
                attributes[prop[0]] = unicode(prop[1])

            self.buffer.append(
                self.client.create_event(event_data)
            )

    def hook_tick(self, brok):

        if self.ticks >= self.tick_limit:
            logger.error("[riemann broker] Buffering ticks exceeded. Freeing buffer")
            self.buffer = []
            self.ticks = 0

        if len(self.buffer) > 0:
            try:
                for event in self.buffer:
                    self.client.send_event(event)
                    self.buffer.remove(event)
                self.ticks = 0
            except:
                self.ticks += 1
                logger.error("[riemann broker] Sending data Failed. Buffering state : %s / %s"
                             % (self.ticks, self.tick_limit))
