# -*- coding: utf-8 -*-

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

require File.expand_path("storm", File.dirname(__FILE__))

$words = ["nathan", "mike", "jackson", "golda", "bertels人"]

def random_word
  $words[rand($words.length)]
end

class TesterSpout < Storm::Spout
  attr_accessor :uid, :pending

  def open(conf, context)
    emit ['spout initializing']
    self.pending = {}
    self.uid = 0
  end

  def nextTuple
    sleep 0.5
    word = random_word
    id = self.uid += 1
    self.pending[id] = word
    emit [word], :id => id
  end

  def ack(id)
    self.pending.delete(id)
  end

  def fail(id)
    word = self.pending[id]
    log "emitting " + word + " on fail"
    emit [word], :id => id
  end
end

TesterSpout.new.run
