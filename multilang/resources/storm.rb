require "rubygems"
require "json"

module Storm
  module Protocol
    def read_string_message
      msg = ""
      loop do
        line = STDIN.readline.chomp
        break if line == "end"
        msg << line
        msg << "\n"
      end
      msg.chomp
    end

    def read_message
      JSON.parse read_string_message
    end

    def send_to_parent(s)
      puts s
      puts "end"
      STDOUT.flush
    end

    def sync
      puts "sync"
      STDOUT.flush
    end

    def send_pid(heartbeat_dir)
      pid = Process.pid
      puts pid
      STDOUT.flush
      File.open("#{heartbeat_dir}/#{pid}", "w").close
    end

    def send_msg_to_parent(hash)
      send_to_parent hash.to_json
    end

    def emit_tuple(tup, stream = nil, anchors = [], direct = nil)
      m = {:command => :emit, :anchors => anchors.map(&:id), :tuple => tup}
      m[:stream] = stream if stream
      m[:task] = direct if direct
      send_msg_to_parent m
    end

    def emit(tup, stream = nil, anchors = [], direct = nil)
      emit_tuple tup, stream, anchors, direct
      read_message
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

    def read_env
      [read_message, read_message]
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

    def initialize(conf = nil, context = nil)
      @conf = conf
      @context = context
    end

    def process(tuple)
    end

    def run
      heartbeat_dir = read_string_message
      send_pid heartbeat_dir
      @conf, @context = read_env
      begin
        while true
          process Tuple.from_hash(read_message)
          sync
        end
      rescue Exception => e
        log e
      end
    end
  end

  class Spout
  end
end
