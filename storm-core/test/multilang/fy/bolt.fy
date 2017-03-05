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

class TestBolt : Storm Bolt {
  def process: tuple {
    emit: $ [tuple values join: ", "]
    ack: tuple
  }
}

FancySpec describe: Storm Bolt with: {
  before_each: {
    Storm Protocol Input clear
    Storm Protocol Output clear
    @storm = Storm Protocol new
    @in = Storm Protocol Input
    @out = Storm Protocol Output
  }

  it: "runs as as expected" for: 'run when: {
    conf = <['some_conf => false]>
    context = <['some_context => true]>
    tup1 = <['id => 1, 'comp => 2, 'stream => 3, 'task => 4, 'tuple => [1,2,3,4]]>
    task_ids_1 = <['task_ids => [1,2,3,4]]> # part of the protocol, random values though
    tup2 = <['id => 2, 'comp => 3, 'stream => 4, 'task => 5, 'tuple => ["hello", "world"]]>
    task_ids_2 = <['task_ids => [2,3,4,5]]> # same here

    @in input: [
      "/tmp/", conf to_json() , context to_json(),
      # tuples:
      tup1 to_json(), task_ids_1 to_json(),
      tup2 to_json(), task_ids_2 to_json()
    ]

    b = TestBolt new
    b run

    @out sent select: |m| {
      m includes?: $ tup1['tuple] join: ", "
    } size is == 1

    @out sent select: |m| {
      m includes?: $ tup2['tuple] join: ", "
    } size is == 1
  }
}