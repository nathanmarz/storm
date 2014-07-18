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
from random import random

class TesterMetricsBolt(storm.Bolt):
    def initialize(self, conf, context):
        storm.log('bolt initializing')

    def process(self, tup):
        word = tup.values[0];
        
        storm.rpcMetrics("my-custom-shell-metric", 1);
        
        storm.ack(tup)

TesterMetricsBolt().run()
