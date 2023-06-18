#!/usr/bin/env python
# -*- coding: utf-8; py-indent-offset:4 -*-
import backtrader as bt
from ..utils import date2num
import datetime as dt
import aerospike as ae

class AerospikeDB(bt.feed.DataBase):
    params = (
        ('host', 'localhost'),
        ('port', '3000'),
        ('use_services_alternate', False),
        ('namespace', None),
        ('set', None),
        ('key', None),
        ('bin', 'Data'),
        ('fromdate', None),
        ('todate', None),
    )

    def start(self):
        super(AerospikeDB, self).start()
        
        # Connect to the Aerospike cluster
        config = {
            'hosts': [(self.p.host, int(self.p.port))],
            'use_services_alternate': self.p.use_services_alternate
        }
        self.client = ae.client(config).connect()

        # If no date is provided, use the current date
        fromdate_u = int(self.p.fromdate.timestamp()) if self.p.fromdate else date2num(dt.datetime.now())
        todate_u = int(self.p.todate.timestamp()) if self.p.todate else date2num(dt.datetime.now())

        # Creates a key
        self.key = (self.p.namespace, self.p.set, self.p.key)

        # Get records
        records = self.client.map_get_by_key_range(self.key, self.p.bin, fromdate_u, todate_u, ae.MAP_RETURN_KEY_VALUE)
        self.records = iter(records)

    def stop(self):
        # Close the connection to the server
        if self.client is not None:
            self.client.close()
            self.client = None

    def _load(self):
        try:
            # Get the next record
            record = next(self.records)
        except StopIteration:
            return False

        # Get key and values from the record
        date, values = record

        # Get the values from the record
        self.lines.datetime[0] = date2num(dt.datetime.fromtimestamp(date))
        self.lines.open[0] = values[0]
        self.lines.high[0] = values[1]
        self.lines.low[0] = values[2]
        self.lines.close[0] = values[3]
        self.lines.volume[0] = values[4]

        return True
