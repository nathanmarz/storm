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

# This Python file uses the following encoding: utf-8

import storm
from random import choice
from time import sleep
from uuid import uuid4

words = [u"nathan", u"mike", u"jackson", u"golda", u"bertels人"]

class TesterSpout(storm.Spout):
    def initialize(self, conf, context):
        storm.log('spout initializing')
        self.pending = {}
        self.count = 0

    def nextTuple(self):
        if self.count < 2:
            word = choice(words)
            id = str(uuid4())
            self.pending[id] = word
            storm.rpcMetrics("my-custom-shellspout-metric", 1)
            self.count = self.count + 1
            storm.log("TesterSpout update my-custom-shellspout-metric "+str(self.count))
            storm.emit([word], id=id)

    def ack(self, id):
        del self.pending[id]

    def fail(self, id):
        storm.log("emitting " + self.pending[id] + " on fail")
        storm.emit([self.pending[id]], id=id)

TesterSpout().run()
