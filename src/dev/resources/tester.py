import storm

class TesterBolt(storm.Bolt):
    def process(self, tup):
        storm.emit([tup.values[0]+"lalala"])
        storm.ack(tup)

TesterBolt().run()