# This Python file uses the following encoding: utf-8

import storm
from random import random

class TesterBolt(storm.Bolt):
    def initialize(self, conf, context):
        storm.emit(['bolt initializing'])

    def process(self, tup):
        word = tup.values[0];
        if (random() < 0.75):
            storm.emit([word + 'lalala'], anchors=[tup])
            storm.ack(tup)
        else:
            storm.log(word + ' randomly skipped!')

TesterBolt().run()
