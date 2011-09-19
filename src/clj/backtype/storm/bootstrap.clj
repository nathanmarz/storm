(ns backtype.storm.bootstrap)

(defmacro bootstrap []
  '(do
      (import (quote [backtype.storm Constants]))
      (import (quote [backtype.storm.testing FeederSpout TestPlannerBolt TestPlannerSpout AckFailDelegate AckTracker]))
      (import (quote [backtype.storm.utils Utils LocalState Time TimeCacheMap
                      TimeCacheMap$ExpiredCallback BufferFileInputStream]))
      (import (quote [backtype.storm.serialization TupleSerializer TupleDeserializer]))
      (import (quote [backtype.storm.spout ISpout SpoutOutputCollector ISpoutOutputCollector ShellSpout]))
      (import (quote [backtype.storm.tuple Tuple Fields MessageId]))
      (import (quote [backtype.storm.task IBolt IOutputCollector
                      OutputCollector OutputCollectorImpl IInternalOutputCollector
                      TopologyContext ShellBolt
                      CoordinatedBolt CoordinatedBolt$SourceArgs KeyedFairBolt]))
      (import (quote [backtype.storm.daemon Shutdownable]))
      (use (quote [backtype.storm config util log clojure]))
      (require (quote [backtype.storm [thrift :as thrift] [cluster :as cluster]
                                      [event :as event] [process-simulator :as psim]]))
      (require (quote [clojure.set :as set]))
      (require (quote [zilch [mq :as mq]]))
      (require (quote [zilch [virtual-port :as mqvp]]))
      (require (quote [backtype.storm [stats :as stats]]))
      (import (quote [org.apache.log4j PropertyConfigurator Logger]))

      (import (quote [backtype.storm.generated Nimbus Nimbus$Processor Nimbus$Iface StormTopology ShellComponent
                        NotAliveException AlreadyAliveException InvalidTopologyException
                        ClusterSummary TopologyInfo TopologySummary TaskSummary TaskStats TaskSpecificStats
                        SpoutStats BoltStats ErrorInfo SupervisorSummary]))
      (import (quote [backtype.storm.daemon.common StormBase Assignment
                      TaskInfo SupervisorInfo WorkerHeartbeat TaskHeartbeat]))
      (import (quote [java.io File FileOutputStream FileInputStream]))
      (import (quote [java.util List Random Map HashMap]))
      (import (quote [org.apache.commons.io FileUtils]))
      (import (quote [java.util ArrayList]))
      (mq/zeromq-imports)
      ))
