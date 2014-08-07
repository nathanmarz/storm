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
import sys
import os
import traceback
from collections import deque

try:
    import simplejson as json
except ImportError:
    import json

json_encode = lambda x: json.dumps(x)
json_decode = lambda x: json.loads(x)

#reads lines and reconstructs newlines appropriately
def readMsg():
    msg = ""
    while True:
        line = sys.stdin.readline()[0:-1]
        if line == "end":
            break
        msg = msg + line + "\n"
    return json_decode(msg[0:-1])

MODE = None
ANCHOR_TUPLE = None

#queue up commands we read while trying to read taskids
pending_commands = deque()

def readTaskIds():
    if pending_taskids:
        return pending_taskids.popleft()
    else:
        msg = readMsg()
        while type(msg) is not list:
            pending_commands.append(msg)
            msg = readMsg()
        return msg

#queue up taskids we read while trying to read commands/tuples
pending_taskids = deque()

def readCommand():
    if pending_commands:
        return pending_commands.popleft()
    else:
        msg = readMsg()
        while type(msg) is list:
            pending_taskids.append(msg)
            msg = readMsg()
        return msg

def readTuple():
    cmd = readCommand()
    return Tuple(cmd["id"], cmd["comp"], cmd["stream"], cmd["task"], cmd["tuple"])

def sendMsgToParent(msg):
    print json_encode(msg)
    print "end"
    sys.stdout.flush()

def sync():
    sendMsgToParent({'command':'sync'})

def sendpid(heartbeatdir):
    pid = os.getpid()
    sendMsgToParent({'pid':pid})
    open(heartbeatdir + "/" + str(pid), "w").close()    

def emit(*args, **kwargs):
    __emit(*args, **kwargs)
    return readTaskIds()

def emitDirect(task, *args, **kwargs):
    kwargs['directTask'] = task
    __emit(*args, **kwargs)

def __emit(*args, **kwargs):
    global MODE
    if MODE == Bolt:
        emitBolt(*args, **kwargs)
    elif MODE == Spout:
        emitSpout(*args, **kwargs)

def emitBolt(tup, stream=None, anchors = [], directTask=None):
    global ANCHOR_TUPLE
    if ANCHOR_TUPLE is not None:
        anchors = [ANCHOR_TUPLE]
    m = {"command": "emit"}
    if stream is not None:
        m["stream"] = stream
    m["anchors"] = map(lambda a: a.id, anchors)
    if directTask is not None:
        m["task"] = directTask
    m["tuple"] = tup
    sendMsgToParent(m)
    
def emitSpout(tup, stream=None, id=None, directTask=None):
    m = {"command": "emit"}
    if id is not None:
        m["id"] = id
    if stream is not None:
        m["stream"] = stream
    if directTask is not None:
        m["task"] = directTask
    m["tuple"] = tup
    sendMsgToParent(m)

def ack(tup):
    sendMsgToParent({"command": "ack", "id": tup.id})

def fail(tup):
    sendMsgToParent({"command": "fail", "id": tup.id})

def log(msg):
    sendMsgToParent({"command": "log", "msg": msg})

def initComponent():
    setupInfo = readMsg()
    sendpid(setupInfo['pidDir'])
    return [setupInfo['conf'], setupInfo['context']]

class Tuple:
    def __init__(self, id, component, stream, task, values):
        self.id = id
        self.component = component
        self.stream = stream
        self.task = task
        self.values = values

    def __repr__(self):
        return '<%s%s>' % (
                self.__class__.__name__,
                ''.join(' %s=%r' % (k, self.__dict__[k]) for k in sorted(self.__dict__.keys())))

class Bolt:
    def initialize(self, stormconf, context):
        pass

    def process(self, tuple):
        pass

    def run(self):
        global MODE
        MODE = Bolt
        conf, context = initComponent()
        self.initialize(conf, context)
        try:
            while True:
                tup = readTuple()
                self.process(tup)
        except Exception, e:
            log(traceback.format_exc(e))

class BasicBolt:
    def initialize(self, stormconf, context):
        pass

    def process(self, tuple):
        pass

    def run(self):
        global MODE
        MODE = Bolt
        global ANCHOR_TUPLE
        conf, context = initComponent()
        self.initialize(conf, context)
        try:
            while True:
                tup = readTuple()
                ANCHOR_TUPLE = tup
                self.process(tup)
                ack(tup)
        except Exception, e:
            log(traceback.format_exc(e))

class Spout:
    def initialize(self, conf, context):
        pass

    def ack(self, id):
        pass

    def fail(self, id):
        pass

    def nextTuple(self):
        pass

    def run(self):
        global MODE
        MODE = Spout
        conf, context = initComponent()
        self.initialize(conf, context)
        try:
            while True:
                msg = readCommand()
                if msg["command"] == "next":
                    self.nextTuple()
                if msg["command"] == "ack":
                    self.ack(msg["id"])
                if msg["command"] == "fail":
                    self.fail(msg["id"])
                sync()
        except Exception, e:
            log(traceback.format_exc(e))
