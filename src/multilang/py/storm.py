import sys
import os
import traceback

try:
    import cjson
    json_encode = cjson.encode
    json_decode = lambda x: cjson.decode(x, all_unicode=True)
except ImportError:
    import json
    json_encode = lambda x: json.dumps(x, ensure_ascii=False)
    json_decode = lambda x: json.loads(unicode(x))

def readStringMsg():
    msg = ""
    while True:
        line = sys.stdin.readline()[0:-1]
        if line == "end":
            break
        msg = msg + line + "\n"
    return msg[0:-1]

ANCHOR_TUPLE = None

#reads lines and reconstructs newlines appropriately
def readMsg():

    msg = readStringMsg()
    
    try:
        msg = json_decode(msg)
    except:
        pass
        
    return msg

def sendToParent(s):
    print s
    print "end"
    sys.stdout.flush()
    
def sync():
    print "sync"
    sys.stdout.flush()

def sendpid(heartbeatdir):
    pid = os.getpid()
    print pid
    sys.stdout.flush()
    open(heartbeatdir + "/" + str(pid), "w").close()    

def sendMsgToParent(amap):
    sendToParent(json_encode(amap))

def emittuple(tup, stream=None, anchors = [], directTask=None):
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
    
def emit(tup, stream=None, anchors = []):
    emittuple(tup, stream=stream, anchors=anchors)
    #read back task ids
    return readMsg()
    
def emitDirect(task, tup, stream=None, anchors = []):
    emittuple(tup, stream=stream, anchors=anchors, directTask=task)

def ack(tup):
    sendMsgToParent({"command": "ack", "id": tup.id})

def fail(tup):
    sendMsgToParent({"command": "fail", "id": tup.id})

def log(msg):
    sendMsgToParent({"command": "log", "msg": msg})

# read the stormconf and context
def readenv():
    conf = readMsg()
    context = readMsg()
    return [conf, context]

def readtuple():
    tupmap = readMsg()
    return Tuple(tupmap["id"], tupmap["comp"], tupmap["stream"], tupmap["task"], tupmap["tuple"])

def initbolt():
    heartbeatdir = readStringMsg()
    sendpid(heartbeatdir)
    return readenv()

def initspout():
    heartbeatdir = readStringMsg()
    sendpid(heartbeatdir)
    return readenv()
    
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
        conf, context = initbolt()
        self.initialize(conf, context)
        try:
            while True:
                tup = readtuple()
                self.process(tup)
                sync()
        except Exception, e:
            log(traceback.format_exc(e))        

class BasicBolt:
    def initialize(self, stormconf, context):
        pass
    
    def process(self, tuple):
        pass
    
    def run(self):
        global ANCHOR_TUPLE
        conf, context = initbolt()
        self.initialize(conf, context)
        try:
            while True:
                tup = readtuple()
                ANCHOR_TUPLE = tup
                self.process(tup)
                ack(tup)
                sync()
        except Exception, e:
            log(traceback.format_exc(e))

class ShellSpout:
    def initialize(self, stormconf, context):
        pass
    
    def nextTuple(self):
        pass
    
    def run(self):
        conf, context = initspout()
        self.initialize(conf, context)
        
        try:
        
            while True:
                msg = readMsg()

                if (msg == 'next'):
                    self.nextTuple()
                elif ('command' in msg):    

                    tup = Tuple(msg["id"], msg["comp"], msg["stream"], msg["task"], msg["tuple"])

                    if (msg['command'] == 'ack'):
                        self.ack(tup)
                    elif (msg['command'] == 'fail'):
                        self.fail(tup)
                
        except Exception, e:	
			log(traceback.format_exc(e))
                
    def ack(self, tup):
        pass

    def fail(self, fail):
        pass