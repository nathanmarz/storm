# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

require: "mocks"

FancySpec describe: Storm Protocol with: {
  before_each: {
    Storm Protocol Input clear
    Storm Protocol Output clear
    @storm = Storm Protocol new
    @in = Storm Protocol Input
    @out = Storm Protocol Output
    @tuple = Storm Tuple new: 1 component: 2 stream: 3 task: 4 values: [1,2,3,4]
  }

  it: "reads a string message correctly" for: 'read_string_message when: {
    @in receive_msg: "/tmp/"
    @storm read_string_message is == "/tmp/"
  }

  it: "reads a json message correctly" for: 'read_message when: {
    @in receive_msg: "{\"foo\":123, \"bar\":\"foobar\", \"tuple\":[1,2,\"cool\"]}"
    msg = @storm read_message
    msg is == <["foo" => 123, "bar" => "foobar", "tuple" => [1,2,"cool"]]>
  }

  it: "sends a message correctly" for: 'send: when: {
    msg = <['hello => "world", 'testing => 42]>
    @storm send: msg
    @out sent is == ["#{msg to_json()}\n", "end\n"]
  }

  it: "sends the pid to the parent process" for: 'send_pid: when: {
    @storm send_pid: "/tmp/"
    pid = Process pid()
    @out sent is == ["#{pid}\n"]
  }

  it: "syncs with the parent process" for: 'sync when: {
    @storm sync
    @out sent is == ["sync\n"]
  }

  it: "emits a tuple to storm" for: 'emit_tuple:stream:anchors:direct: when: {
    tuple_values = ["hello", "world"]
    @storm emit_tuple: tuple_values
    emit_msg = JSON parse(@out sent[-2]) # last one is "end"
    emit_msg is == <["command" => "emit", "anchors" => [], "tuple" => tuple_values]>
  }

  it: "acks a tuple" for: 'ack: when: {
    @storm ack: @tuple
    ack_msg = JSON parse(@out sent[-2])
    ack_msg is == <["command" => "ack", "id" => @tuple id]>
  }

  it: "fails a tuple" for: 'fail: when: {
    @storm fail: @tuple
    fail_msg = JSON parse(@out sent[-2])
    fail_msg is == <["command" => "fail", "id" => @tuple id]>
  }

  it: "logs a message" for: 'log: when: {
    @storm log: "log test"
    log_msg = JSON parse(@out sent[-2])
    log_msg is == <["command" => "log", "msg" => "log test"]>
  }
}