# Storm UI REST API
Storm UI server provides a REST Api to access cluster, topology, component overview and metrics. 
This api returns json response.  
Please ignore undocumented elements in the json repsonse.

## Using the UI REST Api

### /api/v1/cluster/configuration (GET)
 returns cluster configuration.  Below is a sample response but doesn't include all the config fileds.

Sample Response:  
```json
  {
    "dev.zookeeper.path": "/tmp/dev-storm-zookeeper",
    "topology.tick.tuple.freq.secs": null,
    "topology.builtin.metrics.bucket.size.secs": 60,
    "topology.fall.back.on.java.serialization": true,
    "topology.max.error.report.per.interval": 5,
    "zmq.linger.millis": 5000,
    "topology.skip.missing.kryo.registrations": false,
    "storm.messaging.netty.client_worker_threads": 1,
    "ui.childopts": "-Xmx768m",
    "storm.zookeeper.session.timeout": 20000,
    "nimbus.reassign": true,
    "topology.trident.batch.emit.interval.millis": 500,
    "storm.messaging.netty.flush.check.interval.ms": 10,
    "nimbus.monitor.freq.secs": 10,
    "logviewer.childopts": "-Xmx128m",
    "java.library.path": "/usr/local/lib:/opt/local/lib:/usr/lib",
    "topology.executor.send.buffer.size": 1024,
    }
```
    
### /api/v1/cluster/summary (GET)
returns cluster summary such as nimbus uptime,number of supervisors,slots etc..

Response Fields:

|Field  |Value|Description
|---	|---	|---
|stormVersion|String| Storm version|
|nimbusUptime|String| Shows how long the cluster is running|
|supervisors|Integer|  Number of supervisors running|
|slotsTotal| Integer|Total number of available worker slots|
|slotsUsed| Integer| Number of worker slots used|
|slotsFree| Integer |Number of worker slots available|
|executorsTotal| Integer |Total number of executors|
|tasksTotal| Integer |Total tasks|

Sample Response:  
```json
   {
    "stormVersion": "0.9.2-incubating-SNAPSHOT",
    "nimbusUptime": "3m 53s",
    "supervisors": 1,
    "slotsTotal": 4,
    "slotsUsed": 3,
    "slotsFree": 1,
    "executorsTotal": 28,
    "tasksTotal": 28
    }
```
    
### /api/v1/supervisor/summary (GET)
returns all supervisors summary 

Response Fields:

|Field  |Value|Description|
|---	|---	|---
|id| String | Supervisor's id|
|host| String| Supervisor's host name|
|uptime| String| Shows how long the supervisor is running|
|slotsTotal| Integer| Total number of available worker slots for this supervisor|
|slotsUsed| Integer| Number of worker slots used on this supervisor|

Sample Response:  
```json
{
    "supervisors": [
        {
            "id": "0b879808-2a26-442b-8f7d-23101e0c3696",
            "host": "10.11.1.7",
            "uptime": "5m 58s",
            "slotsTotal": 4,
            "slotsUsed": 3
        }
    ]
}
```
    
### /api/v1/topology/summary (GET)
Returns all topologies summary

Response Fields:

|Field  |Value | Description|
|---	|---	|---
|id| String| Topology Id|
|name| String| Topology Name|
|status| String| Topology Status|
|uptime| String|  Shows how long the topology is running|
|tasksTotal| Integer |Total number of tasks for this topology|
|workersTotal| Integer |Number of workers used for this topology|
|executorsTotal| Integer |Number of executors used for this topology|

Sample Response:  
```json
{
    "topologies": [
        {
            "id": "WordCount3-1-1402960825",
            "name": "WordCount3",
            "status": "ACTIVE",
            "uptime": "6m 5s",
            "tasksTotal": 28,
            "workersTotal": 3,
            "executorsTotal": 28
        }
    ]
}
```
 
### /api/v1/topology/:id (GET)
  Returns topology information and stats. Subsititute id with topology id.

Request Parameters:
  
|Parameter |Value   |Description  |
|----------|--------|-------------|
|id   	   |String (required)| Topology Id  |
|window    |String. Default value :all-time| Window duration for metrics in ms|
|sys       |String. Values 1 or 0. Default value 0| Controls including sys stats part of the response|


Response Fields:

|Field  |Value |Description|
|---	|---	|---
|id| String| Topology Id|
|name| String |Topology Name|
|uptime| String |Shows how long the topology is running|
|status| String |Shows Topology's current status|
|tasksTotal| Integer |Total number of tasks for this topology|
|workersTotal| Integer |Number of workers used for this topology|
|executorsTotal| Integer |Number of executors used for this topology|
|msgTimeout| Integer | Number of seconds a tuple has before the spout considers it failed |
|windowHint| String | window param value in "hh mm ss" format. Default value is "All Time"|
|topologyStats| Array | Array of all the topology related stats per time window|
|topologyStats.windowPretty| String |Duration passed in HH:MM:SS format|
|topologyStats.window| String |User requested time window for metrics|
|topologyStats.emitted| Long |Number of messages emitted in given window|
|topologyStats.trasferred| Long |Number messages transferred in given window|
|topologyStats.completeLatency| String (double value returned in String format) |Total latency for processing the message|
|topologyStats.acked| Long |Number of messages acked in given window|
|topologyStats.failed| Long |Number of messages failed in given window|
|spouts| Array | Array of all the spout components in the topology|
|spouts.spoutId| String |Spout id|
|spouts.executors| Integer |Number of executors for the spout|
|spouts.emitted| Long |Number of messages emitted in given window |
|spouts.completeLatency| String (double value returned in String format) |Total latency for processing the message|
|spouts.transferred| Long |Total number of messages  transferred in given window|
|spouts.tasks| Integer |Total number of tasks for the spout|
|spouts.lastError| String |Shows the last error happened in a spout|
|spouts.errorLapsedSecs| Integer | Number of seconds elapsed since that last error happened in a spout|
|spouts.errorWorkerLogLink| String | Link to the worker log that reported the exception |
|spouts.acked| Long |Number of messages acked|
|spouts.failed| Long |Number of messages failed|
|bolts| Array | Array of bolt components in the topology|
|bolts.boltId| String |Bolt id|
|bolts.capacity| String (double value returned in String format) |This value indicates number of messages executed * average execute latency / time window|
|bolts.processLatency| String (double value returned in String format)  |Bolt's average time to ack a message after it's received|
|bolts.executeLatency| String (double value returned in String format) |Average time for bolt's execute method |
|bolts.executors| Integer |Number of executor tasks in the bolt component|
|bolts.tasks| Integer |Number of instances of bolt|
|bolts.acked| Long |Number of tuples acked by the bolt|
|bolts.failed| Long |Number of tuples failed by the bolt|
|bolts.lastError| String |Shows the last error occurred in the bolt|
|bolts.errorLapsedSecs| Integer |Number of seconds elapsed since that last error happened in a bolt|
|bolts.errorWorkerLogLink| String | Link to the worker log that reported the exception |
|bolts.emitted| Long |Number of tuples emitted|



Examples:  
```no-highlight
 1. http://ui-daemon-host-name:8080/api/v1/topology/WordCount3-1-1402960825
 2. http://ui-daemon-host-name:8080/api/v1/topology/WordCount3-1-1402960825?sys=1
 3. http://ui-daemon-host-name:8080/api/v1/topology/WordCount3-1-1402960825?window=600
```

Sample Response:  
```json 
 {
    "name": "WordCount3",
    "id": "WordCount3-1-1402960825",
    "workersTotal": 3,
    "window": "600",
    "status": "ACTIVE",
    "tasksTotal": 28,
    "executorsTotal": 28,
    "uptime": "29m 19s",
    "msgTimeout": 30,
    "windowHint": "10m 0s",
    "topologyStats": [
        {
            "windowPretty": "10m 0s",
            "window": "600",
            "emitted": 397960,
            "transferred": 213380,
            "completeLatency": "0.000",
            "acked": 213460,
            "failed": 0
        },
        {
            "windowPretty": "3h 0m 0s",
            "window": "10800",
            "emitted": 1190260,
            "transferred": 638260,
            "completeLatency": "0.000",
            "acked": 638280,
            "failed": 0
        },
        {
            "windowPretty": "1d 0h 0m 0s",
            "window": "86400",
            "emitted": 1190260,
            "transferred": 638260,
            "completeLatency": "0.000",
            "acked": 638280,
            "failed": 0
        },
        {
            "windowPretty": "All time",
            "window": ":all-time",
            "emitted": 1190260,
            "transferred": 638260,
            "completeLatency": "0.000",
            "acked": 638280,
            "failed": 0
        }
    ],
    "spouts": [
        {
            "executors": 5,
            "emitted": 28880,
            "completeLatency": "0.000",
            "transferred": 28880,
            "acked": 0,
            "spoutId": "spout",
            "tasks": 5,
            "lastError": "",
            "errorLapsedSecs": null
            "failed": 0
        }
    ],
        "bolts": [
        {
            "executors": 12,
            "emitted": 184580,
            "transferred": 0,
            "acked": 184640,
            "executeLatency": "0.048",
            "tasks": 12,
            "executed": 184620,
            "processLatency": "0.043",
            "boltId": "count",
            "lastError": "",
            "errorLapsedSecs": null
            "capacity": "0.003",
            "failed": 0
        },
        {
            "executors": 8,
            "emitted": 184500,
            "transferred": 184500,
            "acked": 28820,
            "executeLatency": "0.024",
            "tasks": 8,
            "executed": 28780,
            "processLatency": "2.112",
            "boltId": "split",
            "lastError": "",
            "errorLapsedSecs": null
            "capacity": "0.000",
            "failed": 0
        }
    ],
    "configuration": {
        "storm.id": "WordCount3-1-1402960825",
        "dev.zookeeper.path": "/tmp/dev-storm-zookeeper",
        "topology.tick.tuple.freq.secs": null,
        "topology.builtin.metrics.bucket.size.secs": 60,
        "topology.fall.back.on.java.serialization": true,
        "topology.max.error.report.per.interval": 5,
        "zmq.linger.millis": 5000,
        "topology.skip.missing.kryo.registrations": false,
        "storm.messaging.netty.client_worker_threads": 1,
        "ui.childopts": "-Xmx768m",
        "storm.zookeeper.session.timeout": 20000,
        "nimbus.reassign": true,
        "topology.trident.batch.emit.interval.millis": 500,
        "storm.messaging.netty.flush.check.interval.ms": 10,
        "nimbus.monitor.freq.secs": 10,
        "logviewer.childopts": "-Xmx128m",
        "java.library.path": "/usr/local/lib:/opt/local/lib:/usr/lib",
        "topology.executor.send.buffer.size": 1024,
        "storm.local.dir": "storm-local",
        "storm.messaging.netty.buffer_size": 5242880,
        "supervisor.worker.start.timeout.secs": 120,
        "topology.enable.message.timeouts": true,
        "nimbus.cleanup.inbox.freq.secs": 600,
        "nimbus.inbox.jar.expiration.secs": 3600,
        "drpc.worker.threads": 64,
        "topology.worker.shared.thread.pool.size": 4,
        "nimbus.host": "hw10843.local",
        "storm.messaging.netty.min_wait_ms": 100,
        "storm.zookeeper.port": 2181,
        "transactional.zookeeper.port": null,
        "topology.executor.receive.buffer.size": 1024,
        "transactional.zookeeper.servers": null,
        "storm.zookeeper.root": "/storm",
        "storm.zookeeper.retry.intervalceiling.millis": 30000,
        "supervisor.enable": true,
        "storm.messaging.netty.server_worker_threads": 1
    },
}
```
  

### /api/v1/topology/:id/component/:component (GET)

Returns detailed metrics and executor information

|Parameter |Value   |Description  |
|----------|--------|-------------|
|id   	   |String (required)| Topology Id  |
|component |String (required)| Component Id |
|window    |String. Default value :all-time| window duration for metrics in ms|
|sys       |String. Values 1 or 0. Default value 0| controls including sys stats part of the response|

Response Fields:

|Field  |Value |Description|
|---	|---	|---
|id   | String | Component's id|
|name | String | Topology name|
|componentType | String | component's type SPOUT or BOLT|
|windowHint| String | window param value in "hh mm ss" format. Default value is "All Time"|
|executors| Integer |Number of executor tasks in the component|
|componentErrors| Array of Errors | List of component errors|
|componentErrors.time| Long | Timestamp when the exception occurred |
|componentErrors.errorHost| String | host name for the error|
|componentErrors.errorPort| String | port for the error|
|componentErrors.error| String |Shows the error happened in a component|
|componentErrors.errorLapsedSecs| Integer | Number of seconds elapsed since the error happened in a component |
|componentErrors.errorWorkerLogLink| String | Link to the worker log that reported the exception |
|topologyId| String | Topology's Id|
|tasks| Integer |Number of instances of component|
|window    |String. Default value "All Time" | window duration for metrics in seconds|
|spoutSummary or boltStats| Array |Array of component stats. **Please note this element tag can be spoutSummary or boltStats depending on the componentType**|
|spoutSummary.windowPretty| String |Duration passed in HH:MM:SS format|
|spoutSummary.window| String | window duration for metrics in seconds|
|spoutSummary.emitted| Long |Number of messages emitted in given window |
|spoutSummary.completeLatency| String (double value returned in String format) |Total latency for processing the message|
|spoutSummary.transferred| Long |Total number of messages  transferred in given window|
|spoutSummary.acked| Long |Number of messages acked|
|spoutSummary.failed| Long |Number of messages failed|
|boltStats.windowPretty| String |Duration passed in HH:MM:SS format|
|boltStats..window| String | window duration for metrics in seconds|
|boltStats.transferred| Long |Total number of messages  transferred in given window|
|boltStats.processLatency| String (double value returned in String format)  |Bolt's average time to ack a message after it's received|
|boltStats.acked| Long |Number of messages acked|
|boltStats.failed| Long |Number of messages failed|

Examples:  
```no-highlight
1. http://ui-daemon-host-name:8080/api/v1/topology/WordCount3-1-1402960825/component/spout
2. http://ui-daemon-host-name:8080/api/v1/topology/WordCount3-1-1402960825/component/spout?sys=1
3. http://ui-daemon-host-name:8080/api/v1/topology/WordCount3-1-1402960825/component/spout?window=600
```
 
Sample Response:  
```json
{
    "name": "WordCount3", 
    "id": "spout",
    "componentType": "spout",
    "windowHint": "10m 0s",
    "executors": 5,
    "componentErrors":[{"time": 1406006074000,
                        "errorHost": "10.11.1.70",
                        "errorPort": 6701,
                        "errorWorkerLogLink": "http://10.11.1.7:8000/log?file=worker-6701.log",
                        "errorLapsedSecs": 16,
                        "error": "java.lang.RuntimeException: java.lang.StringIndexOutOfBoundsException: Some Error\n\tat backtype.storm.utils.DisruptorQueue.consumeBatchToCursor(DisruptorQueue.java:128)\n\tat backtype.storm.utils.DisruptorQueue.consumeBatchWhenAvailable(DisruptorQueue.java:99)\n\tat backtype.storm.disruptor$consume_batch_when_available.invoke(disruptor.clj:80)\n\tat backtype...more.."
    }],
    "topologyId": "WordCount3-1-1402960825",
    "tasks": 5,
    "window": "600",
    "spoutSummary": [
        {
            "windowPretty": "10m 0s",
            "window": "600",
            "emitted": 28500,
            "transferred": 28460,
            "completeLatency": "0.000",
            "acked": 0,
            "failed": 0
        },
        {
            "windowPretty": "3h 0m 0s",
            "window": "10800",
            "emitted": 127640,
            "transferred": 127440,
            "completeLatency": "0.000",
            "acked": 0,
            "failed": 0
        },
        {
            "windowPretty": "1d 0h 0m 0s",
            "window": "86400",
            "emitted": 127640,
            "transferred": 127440,
            "completeLatency": "0.000",
            "acked": 0,
            "failed": 0
        },
        {
            "windowPretty": "All time",
            "window": ":all-time",
            "emitted": 127640,
            "transferred": 127440,
            "completeLatency": "0.000",
            "acked": 0,
            "failed": 0
        }
    ],
    "outputStats": [
        {
            "stream": "__metrics",
            "emitted": 40,
            "transferred": 0,
            "completeLatency": "0",
            "acked": 0,
            "failed": 0
        },
        {
            "stream": "default",
            "emitted": 28460,
            "transferred": 28460,
            "completeLatency": "0",
            "acked": 0,
            "failed": 0
        }
    ]
    "executorStats": [
        {
            "workerLogLink": "http://10.11.1.7:8000/log?file=worker-6701.log",
            "emitted": 5720,
            "port": 6701,
            "completeLatency": "0.000",
            "transferred": 5720,
            "host": "10.11.1.7",
            "acked": 0,
            "uptime": "43m 4s",
            "id": "[24-24]",
            "failed": 0
        },
        {
            "workerLogLink": "http://10.11.1.7:8000/log?file=worker-6703.log",
            "emitted": 5700,
            "port": 6703,
            "completeLatency": "0.000",
            "transferred": 5700,
            "host": "10.11.1.7",
            "acked": 0,
            "uptime": "42m 57s",
            "id": "[25-25]",
            "failed": 0
        },
        {
            "workerLogLink": "http://10.11.1.7:8000/log?file=worker-6702.log",
            "emitted": 5700,
            "port": 6702,
            "completeLatency": "0.000",
            "transferred": 5680,
            "host": "10.11.1.7",
            "acked": 0,
            "uptime": "42m 57s",
            "id": "[26-26]",
            "failed": 0
        },
        {
            "workerLogLink": "http://10.11.1.7:8000/log?file=worker-6701.log",
            "emitted": 5700,
            "port": 6701,
            "completeLatency": "0.000",
            "transferred": 5680,
            "host": "10.11.1.7",
            "acked": 0,
            "uptime": "43m 4s",
            "id": "[27-27]",
            "failed": 0
        },
        {
            "workerLogLink": "http://10.11.1.7:8000/log?file=worker-6703.log",
            "emitted": 5680,
            "port": 6703,
            "completeLatency": "0.000",
            "transferred": 5680,
            "host": "10.11.1.7",
            "acked": 0,
            "uptime": "42m 57s",
            "id": "[28-28]",
            "failed": 0
        }
    ]
}
```

### /api/v1/topology/:id/activate (POST)
activates a  topology 

|Parameter |Value   |Description  |
|----------|--------|-------------|
|id   	   |String (required)| Topology Id  |

### /api/v1/topology/:id/deactivate (POST)
deactivates a  topology 

|Parameter |Value   |Description  |
|----------|--------|-------------|
|id   	   |String (required)| Topology Id  |


### /api/v1/topology/:id/rebalance/:wait-time (POST)
rebalances a topology

|Parameter |Value   |Description  |
|----------|--------|-------------|
|id   	   |String (required)| Topology Id  |
|wait-time |String (required)| Wait time before rebalance happens |

### /api/v1/topology/:id/kill/:wait-time (POST)
kills a topology

|Parameter |Value   |Description  |
|----------|--------|-------------|
|id   	   |String (required)| Topology Id  |
|wait-time |String (required)| Wait time before rebalance happens |



### ERRORS
on errors in any of the above api returns 500 http status code with 
the following response.

Sample Response:  
```json
{
  "error": "Internal Server Error",
  "errorMessage": "java.lang.NullPointerException\n\tat clojure.core$name.invoke(core.clj:1505)\n\tat backtype.storm.ui.core$component_page.invoke(core.clj:752)\n\tat backtype.storm.ui.core$fn__7766.invoke(core.clj:782)\n\tat compojure.core$make_route$fn__5755.invoke(core.clj:93)\n\tat compojure.core$if_route$fn__5743.invoke(core.clj:39)\n\tat compojure.core$if_method$fn__5736.invoke(core.clj:24)\n\tat compojure.core$routing$fn__5761.invoke(core.clj:106)\n\tat clojure.core$some.invoke(core.clj:2443)\n\tat compojure.core$routing.doInvoke(core.clj:106)\n\tat clojure.lang.RestFn.applyTo(RestFn.java:139)\n\tat clojure.core$apply.invoke(core.clj:619)\n\tat compojure.core$routes$fn__5765.invoke(core.clj:111)\n\tat ring.middleware.reload$wrap_reload$fn__6880.invoke(reload.clj:14)\n\tat backtype.storm.ui.core$catch_errors$fn__7800.invoke(core.clj:836)\n\tat ring.middleware.keyword_params$wrap_keyword_params$fn__6319.invoke(keyword_params.clj:27)\n\tat ring.middleware.nested_params$wrap_nested_params$fn__6358.invoke(nested_params.clj:65)\n\tat ring.middleware.params$wrap_params$fn__6291.invoke(params.clj:55)\n\tat ring.middleware.multipart_params$wrap_multipart_params$fn__6386.invoke(multipart_params.clj:103)\n\tat ring.middleware.flash$wrap_flash$fn__6675.invoke(flash.clj:14)\n\tat ring.middleware.session$wrap_session$fn__6664.invoke(session.clj:43)\n\tat ring.middleware.cookies$wrap_cookies$fn__6595.invoke(cookies.clj:160)\n\tat ring.adapter.jetty$proxy_handler$fn__6112.invoke(jetty.clj:16)\n\tat ring.adapter.jetty.proxy$org.mortbay.jetty.handler.AbstractHandler$0.handle(Unknown Source)\n\tat org.mortbay.jetty.handler.HandlerWrapper.handle(HandlerWrapper.java:152)\n\tat org.mortbay.jetty.Server.handle(Server.java:326)\n\tat org.mortbay.jetty.HttpConnection.handleRequest(HttpConnection.java:542)\n\tat org.mortbay.jetty.HttpConnection$RequestHandler.headerComplete(HttpConnection.java:928)\n\tat org.mortbay.jetty.HttpParser.parseNext(HttpParser.java:549)\n\tat org.mortbay.jetty.HttpParser.parseAvailable(HttpParser.java:212)\n\tat org.mortbay.jetty.HttpConnection.handle(HttpConnection.java:404)\n\tat org.mortbay.jetty.bio.SocketConnector$Connection.run(SocketConnector.java:228)\n\tat org.mortbay.thread.QueuedThreadPool$PoolThread.run(QueuedThreadPool.java:582)\n"
}
```
