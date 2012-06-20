require: "storm"

class Testerbolth : Storm bolth {
  def process: tuple {
    emit: [tuple values first + "lalala"]
    ack: tuple
  }
}

Testerbolth new run