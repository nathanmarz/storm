;; Licensed to the Apache Software Foundation (ASF) under one
;; or more contributor license agreements.  See the NOTICE file
;; distributed with this work for additional information
;; regarding copyright ownership.  The ASF licenses this file
;; to you under the Apache License, Version 2.0 (the
;; "License"); you may not use this file except in compliance
;; with the License.  You may obtain a copy of the License at
;;
;; http://www.apache.org/licenses/LICENSE-2.0
;;
;; Unless required by applicable law or agreed to in writing, software
;; distributed under the License is distributed on an "AS IS" BASIS,
;; WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
;; See the License for the specific language governing permissions and
;; limitations under the License.
(ns backtype.storm.bootstrap)

(defmacro bootstrap []
  '(do
     (import (quote [backtype.storm Constants]))
     (import (quote [backtype.storm.testing FeederSpout TestPlannerBolt TestPlannerSpout
                     AckFailDelegate AckTracker]))
     (import (quote [backtype.storm.utils Utils LocalState Time TimeCacheMap
                     TimeCacheMap$ExpiredCallback
                     RotatingMap RotatingMap$ExpiredCallback
                     BufferFileInputStream
                     RegisteredGlobalState ThriftTopologyUtils DisruptorQueue
                     MutableObject MutableLong]))
     (import (quote [backtype.storm.serialization KryoTupleSerializer KryoTupleDeserializer]))
     (import (quote [backtype.storm.spout ISpout SpoutOutputCollector ISpoutOutputCollector ShellSpout]))
     (import (quote [backtype.storm.tuple Tuple TupleImpl Fields MessageId]))
     (import (quote [backtype.storm.task IBolt IOutputCollector
                     OutputCollector TopologyContext ShellBolt
                     GeneralTopologyContext WorkerTopologyContext]))
     (import (quote [backtype.storm.coordination CoordinatedBolt CoordinatedBolt$SourceArgs 
                     IBatchBolt BatchBoltExecutor]))
     (import (quote [backtype.storm.drpc KeyedFairBolt]))
     (import (quote [backtype.storm.daemon Shutdownable]))
     (require (quote [backtype.storm.messaging.loader :as msg-loader]))
     (use (quote [backtype.storm config util log clojure timer]))
     (require (quote [backtype.storm [thrift :as thrift] [cluster :as cluster]
                      [event :as event] [process-simulator :as psim]]))
     (require (quote [clojure.set :as set]))
     (require (quote [backtype.storm [stats :as stats] [disruptor :as disruptor]]))
     (import (quote [org.slf4j Logger]))

     (import (quote [com.lmax.disruptor InsufficientCapacityException]))
     (import (quote [backtype.storm.generated Nimbus Nimbus$Processor
                     Nimbus$Iface StormTopology ShellComponent
                     NotAliveException AlreadyAliveException GlobalStreamId
                     InvalidTopologyException ClusterSummary TopologyInfo
                     TopologySummary ExecutorSummary ExecutorStats ExecutorSpecificStats
                     SpoutStats BoltStats ErrorInfo SupervisorSummary ExecutorInfo
                     KillOptions SubmitOptions RebalanceOptions JavaObject JavaObjectArg
                     TopologyInitialStatus]))
     (import (quote [backtype.storm.daemon.common StormBase Assignment
                     SupervisorInfo WorkerHeartbeat]))
     (import (quote [backtype.storm.grouping CustomStreamGrouping]))
     (import (quote [java.io File FileOutputStream FileInputStream]))
     (import (quote [java.util Collection List Random Map HashMap Collections ArrayList LinkedList]))
     (import (quote [org.apache.commons.io FileUtils]))
     ))
