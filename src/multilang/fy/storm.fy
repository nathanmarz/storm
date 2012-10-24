require("rubygems")
require("json")

class Storm {
  class Protocol {
    """
    Storm Protocol class.
    Contains all methods implementing the Storm multilang protocol using stdio.
    """

    Input = STDIN
    Output = STDOUT

    def read_string_message {
      """
      @return @String@ message send by the parent Storm process.
      """

      msg = ""
      loop: {
        line = Input readline chomp
        { break } if: (line == "end")
        msg << line
        msg << "\n"
      }
      msg chomp
    }

    def read_message {
      """
      @return @Hash@ that is a JSON parsed message from the parent Storm process.
      """

      JSON parse(read_string_message)
    }

    def send: message {
      """
      @message Message to be sent to the parent Storm process converted to JSON.

      Sends a message as JSON to the parent Storm process.
      """

      Output println: $ message to_json()
      Output println: "end"
      Output flush
    }

    def sync {
      Output println: "sync"
      Output flush
    }

    def send_pid: heartbeat_dir {
      pid = Process pid()
      Output println: pid
      Output flush
      File open(heartbeat_dir ++ "/" ++ pid, "w") close
    }

    def emit_tuple: tup stream: stream (nil) anchors: anchors ([]) direct: direct (nil) {
      m = <['command => 'emit, 'anchors => anchors map: 'id, 'tuple => tup]>
      { m['stream]: stream } if: stream
      { m['task]: direct } if: direct
      send: m
    }

    def emit: tup stream: stream (nil) anchors: anchors ([]) direct: direct (nil) {
      emit_tuple: tup stream: stream anchors: anchors direct: direct
      read_message
    }

    def ack: tuple {
      """
      @tuple @Storm Tuple@ to be acked by Storm.
      """

      send: <['command => 'ack, 'id => tuple id]>
    }

    def fail: tuple {
      """
      @tuple @Storm Tuple@ to be failed by Storm.
      """

      send: <['command => 'fail, 'id => tuple id]>
    }

    def reportError: message {
      """
      @message Error mesasge to be reported to Storm
      """

      send: <['command => 'error, 'msg => message to_s]>
    }

    def log: message {
      """
      @message Message to be logged by Storm.
      """

      send: <['command => 'log, 'msg => message to_s]>
    }

    def read_env {
      """
      @return @Tuple@ of Storm (config, context).
      """

      (read_message, read_message)
    }
  }

  class Tuple {
    """
    Tuples are used by storm as principal data component sent between bolts and emitted by spouts.
    Contains a unique id, the component, stream and task it came from and the values associated with it.
    """

    read_write_slots: [ 'id, 'component, 'stream, 'task, 'values ]

    def initialize: @id component: @component stream: @stream task: @task values: @values {}

    def Tuple from_hash: hash {
      """
      @hash @Hash@ of values to be used for a new @Storm Tuple@.
      @return A new @Storm Tuple@ based on the values in @hash.

      Helper method to create a new tuple from a @Hash@.
      """

      id, component, stream, task, values = hash values_at: ("id", "comp", "stream", "task", "tuple")
      Tuple new: id component: component stream: stream task: task values: values
    }
  }

  class Bolt {
    """
    Bolts represent the actual work processes that receive tuples and
    emit new @Storm Tuple@s on their output stream (possible consumed by other Bolts).
    """

    include: Storm Protocol

    def initialize: @conf (nil) context: @context (nil) {}

    def process: tuple {}

    def run {
      """
      Runs the bolt, causing it to receive messages, perform work defined in @Bolt#run
      and possibly emit new messages (@Storm Tuple@s).
      """

      heartbeat_dir = read_string_message
      send_pid: heartbeat_dir
      @conf, @context = read_env
      try {
        loop: {
          process: $ Tuple from_hash: read_message
          sync
        }
      } catch Exception => e {
        reportError: e
      }
    }
  }

  class Spout {
  }
}
