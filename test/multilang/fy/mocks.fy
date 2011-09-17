require: "../../../src/multilang/fy/storm"

class MockedIO {
  def initialize {
    @out = []
    @in = []
  }

  def print: string {
    @out << (string to_s)
  }

  def println: string {
    @out << (string ++ "\n")
  }

  def input: input {
    input each: |i| {
      @in << (i ++ "\n")
      @in << "end\n"
    }
  }

  def readline {
    if: (@in empty?) then: {
      "No input left" raise!
    }
    @in shift
  }

  def receive_msg: msg {
    @in << (msg ++ "\n")
    @in << "end\n"
  }

  def clear {
    @in = []
    @out = []
  }

  def flush {
  }

  def received {
    @in
  }

  def sent {
    @out
  }
}

class Storm Protocol {
  Input = MockedIO new
  Output = MockedIO new
}