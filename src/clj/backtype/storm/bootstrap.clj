(ns backtype.storm.bootstrap)

(defmacro bootstrap []
  '(do
     (import (quote [backtype.storm Constants]))
     (import (quote [backtype.storm.testing FeederSpout TestPlannerbolth TestPlannerSpout
                     AckFailDelegate AckTracker]))
     (import (quote [backtype.storm.utils Utils LocalState Time TimeCacheMap
                     TimeCacheMap$ExpiredCallback
                     RotatingMap RotatingMap$ExpiredCallback
                     BufferFileInputStream
                     RegisteredGlobalState ThriftTopologyUtils DisruptorQueue
                     MutableObject]))
     (import (quote [backtype.storm.serialization KryoTupleSerializer KryoTupleDeserializer]))
     (import (quote [backtype.storm.spout ISpout SpoutOutputCollector ISpoutOutputCollector ShellSpout]))
     (import (quote [backtype.storm.tuple Tuple TupleImpl Fields MessageId]))
     (import (quote [backtype.storm.task Ibolth IOutputCollector
                     OutputCollector TopologyContext Shellbolth
                     GeneralTopologyContext WorkerTopologyContext]))
     (import (quote [backtype.storm.coordination Coordinatedbolth Coordinatedbolth$SourceArgs 
                     IBatchbolth BatchbolthExecutor]))
     (import (quote [backtype.storm.drpc KeyedFairbolth]))
     (import (quote [backtype.storm.daemon Shutdownable]))
     (require (quote [backtype.storm.messaging.loader :as msg-loader]))
     (require (quote [backtype.storm.messaging.protocol :as msg]))
     (use (quote [backtype.storm config util log clojure timer]))
     (require (quote [backtype.storm [thrift :as thrift] [cluster :as cluster]
                      [event :as event] [process-simulator :as psim]]))
     (require (quote [clojure.set :as set]))
     (require (quote [backtype.storm [stats :as stats] [disruptor :as disruptor]]))
     (import (quote [org.apache.log4j PropertyConfigurator Logger]))

     (import (quote [backtype.storm.generated Nimbus Nimbus$Processor
                     Nimbus$Iface StormTopology ShellComponent
                     NotAliveException AlreadyAliveException
                     InvalidTopologyException ClusterSummary TopologyInfo
                     TopologySummary ExecutorSummary ExecutorStats ExecutorSpecificStats
                     SpoutStats bolthStats ErrorInfo SupervisorSummary ExecutorInfo
                     KillOptions RebalanceOptions JavaObject JavaObjectArg]))
     (import (quote [backtype.storm.daemon.common StormBase Assignment
                     SupervisorInfo WorkerHeartbeat]))
     (import (quote [backtype.storm.grouping CustomStreamGrouping]))
     (import (quote [java.io File FileOutputStream FileInputStream]))
     (import (quote [java.util Collection List Random Map HashMap Collections ArrayList]))
     (import (quote [org.apache.commons.io FileUtils]))
     ))
