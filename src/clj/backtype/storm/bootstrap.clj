(ns backtype.storm.bootstrap)

(defmacro bootstrap []
  '(do
     (import (quote [backtype.storm Constants]))
     (import (quote [backtype.storm.testing FeederSpout TestPlannerBolt TestPlannerSpout
                     AckFailDelegate AckTracker]))
     (import (quote [backtype.storm.utils Utils LocalState Time TimeCacheMap
                     TimeCacheMap$ExpiredCallback BufferFileInputStream
                     RegisteredGlobalState ThriftTopologyUtils]))
     (import (quote [backtype.storm.serialization KryoTupleSerializer KryoTupleDeserializer]))
     (import (quote [backtype.storm.spout ISpout SpoutOutputCollector ISpoutOutputCollector ShellSpout]))
     (import (quote [backtype.storm.tuple Tuple Fields MessageId]))
     (import (quote [backtype.storm.task IBolt IOutputCollector
                     OutputCollector TopologyContext ShellBolt
                     GeneralTopologyContext WorkerTopologyContext]))
     (import (quote [backtype.storm.coordination CoordinatedBolt CoordinatedBolt$SourceArgs 
                     IBatchBolt BatchBoltExecutor]))
     (import (quote [backtype.storm.drpc KeyedFairBolt]))
     (import (quote [backtype.storm.daemon Shutdownable]))
     (require (quote [backtype.storm.messaging.loader :as msg-loader]))
     (require (quote [backtype.storm.messaging.protocol :as msg]))
     (use (quote [backtype.storm config util log clojure timer]))
     (require (quote [backtype.storm [thrift :as thrift] [cluster :as cluster]
                      [event :as event] [process-simulator :as psim]]))
     (require (quote [clojure.set :as set]))
     (require (quote [backtype.storm [stats :as stats]]))
     (import (quote [org.apache.log4j PropertyConfigurator Logger]))

     (import (quote [backtype.storm.generated Nimbus Nimbus$Processor
                     Nimbus$Iface StormTopology ShellComponent
                     NotAliveException AlreadyAliveException
                     InvalidTopologyException ClusterSummary TopologyInfo
                     TopologySummary ExecutorSummary ExecutorStats ExecutorSpecificStats
                     SpoutStats BoltStats ErrorInfo SupervisorSummary ExecutorInfo
                     KillOptions RebalanceOptions JavaObject JavaObjectArg]))
     (import (quote [backtype.storm.daemon.common StormBase Assignment
                     SupervisorInfo WorkerHeartbeat]))
     (import (quote [backtype.storm.grouping CustomStreamGrouping]))
     (import (quote [java.io File FileOutputStream FileInputStream]))
     (import (quote [java.util List Random Map HashMap Collections ArrayList]))
     (import (quote [org.apache.commons.io FileUtils]))
     ))
