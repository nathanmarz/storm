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