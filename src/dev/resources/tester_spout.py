from storm import Spout, emit, log
from random import choice
from time import sleep
from uuid import uuid4

words = ["nathan", "mike", "jackson", "golda", "bertels"]

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
        emit([self.pending[id]], id=id)

TesterSpout().run()
