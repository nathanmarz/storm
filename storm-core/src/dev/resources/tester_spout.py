# This Python file uses the following encoding: utf-8

from storm import Spout, emit, log
from random import choice
from time import sleep
from uuid import uuid4

words = [u"nathan", u"mike", u"jackson", u"golda", u"bertels人"]

class TesterSpout(Spout):
    def initialize(self, conf, context):
        emit(['spout initializing'])
        self.pending = {}

    def nextTuple(self):
        sleep(1.0/2)
        word = choice(words)
        id = str(uuid4())
        self.pending[id] = word
        emit([word], id=id)

    def ack(self, id):
        del self.pending[id]

    def fail(self, id):
        log("emitting " + self.pending[id] + " on fail")
        emit([self.pending[id]], id=id)

TesterSpout().run()
