require: "storm"

class TesterBolt : Storm Bolt {
  def process: tuple {
    emit: [tuple values first + "lalala"]
    ack: tuple
  }
}

TesterBolt new run