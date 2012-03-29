require "rubygems"
require "json"

module Storm
  module Protocol
    class << self
      attr_accessor :mode, :pending_taskids, :pending_commands
    end

    self.pending_taskids = []
    self.pending_commands = []

    def read_message
      msg = ""
      loop do
        line = STDIN.readline.chomp
        break if line == "end"
        msg << line
        msg << "\n"
      end
      JSON.parse msg.chomp
    end

    def read_task_ids
      Storm::Protocol.pending_taskids.shift ||
        begin
          msg = read_message
          until msg.is_a? Array
            Storm::Protocol.pending_commands.push(msg)
            msg = read_message
          end
          msg
        end
    end

    def read_command
      Storm::Protocol.pending_commands.shift ||
        begin
          msg = read_message
          while msg.is_a? Array
            Storm::Protocol.pending_taskids.push(msg)
            msg = read_message
          end
          msg
        end
    end

    def send_msg_to_parent(msg)
      puts msg.to_json
      puts "end"
      STDOUT.flush
    end

    def sync
      send_msg_to_parent({'command' => 'sync'})
    end

    def send_pid(heartbeat_dir)
      pid = Process.pid
      send_msg_to_parent({'pid' => pid})
      File.open("#{heartbeat_dir}/#{pid}", "w").close
    end

    def emit_bolt(tup, args = {})
      stream = args[:stream]
      anchors = args[:anchors] || args[:anchor] || []
      anchors = [anchors] unless anchors.is_a? Enumerable
      direct = args[:direct_task]
      m = {:command => :emit, :anchors => anchors.map(&:id), :tuple => tup}
      m[:stream] = stream if stream
      m[:task] = direct if direct
      send_msg_to_parent m
      read_task_ids unless direct
    end

    def emit_spout(tup, args = {})
      stream = args[:stream]
      id = args[:id]
      direct = args[:direct_task]
      m = {:command => :emit, :tuple => tup}
      m[:id] = id if id
      m[:stream] = stream if stream
      m[:task] = direct if direct
      send_msg_to_parent m
      read_task_ids unless direct
    end

    def emit(*args)
      case Storm::Protocol.mode
      when 'spout'
        emit_spout(*args)
      when 'bolt'
        emit_bolt(*args)
      end
    end

    def ack(tup)
      send_msg_to_parent :command => :ack, :id => tup.id
    end

    def fail(tup)
      send_msg_to_parent :command => :fail, :id => tup.id
    end

    def log(msg)
      send_msg_to_parent :command => :log, :msg => msg.to_s
    end

    def handshake
      setup_info = read_message
      send_pid setup_info['pidDir']
      [setup_info['conf'], setup_info['context']]
    end
  end

  class Tuple
    attr_accessor :id, :component, :stream, :task, :values

    def initialize(id, component, stream, task, values)
      @id = id
      @component = component
      @stream = stream
      @task = task
      @values = values
    end

    def self.from_hash(hash)
      Tuple.new(*hash.values_at("id", "comp", "stream", "task", "tuple"))
    end
  end

  class Bolt
    include Storm::Protocol

    def prepare(conf, context); end

    def process(tuple); end

    def run
      Storm::Protocol.mode = 'bolt'
      prepare(*handshake)
      begin
        while true
          process Tuple.from_hash(read_command)
        end
      rescue Exception => e
        log 'Exception in bolt: ' + e.message + ' - ' + e.backtrace.join('\n')
      end
    end
  end

  class Spout
    include Storm::Protocol

    def open(conf, context); end

    def nextTuple; end

    def ack(id); end

    def fail(id); end

    def run
      Storm::Protocol.mode = 'spout'
      open(*handshake)

      begin
        while true
          msg = read_command
          case msg['command']
          when 'next'
            nextTuple
          when 'ack'
            ack(msg['id'])
          when 'fail'
            fail(msg['id'])
          end
          sync
        end
      rescue Exception => e
        log 'Exception in spout: ' + e.message + ' - ' + e.backtrace.join('\n')
      end
    end
  end
end
