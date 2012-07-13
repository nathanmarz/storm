(ns backtype.storm.testing4j
  (:import [java.util Map List Collection ArrayList])
  (:import [backtype.storm Config])
  (:import [backtype.storm.generated StormTopology])
  (:import [backtype.storm.testing TestJob MockedSources])
  (:import [backtype.storm.utils Utils])
  (:use [backtype.storm testing util log])
  (:gen-class
   :name backtype.storm.Testing
   :methods [^:static [completeTopology
                       [java.util.Map backtype.storm.generated.StormTopology
                        backtype.storm.testing.MockedSources backtype.storm.Config Boolean String]
                       java.util.Map]
             ^:static [completeTopology
                       [java.util.Map backtype.storm.generated.StormTopology
                        backtype.storm.testing.MockedSources backtype.storm.Config Boolean]
                       java.util.Map]
             ^:static [completeTopology
                       [java.util.Map backtype.storm.generated.StormTopology
                        backtype.storm.testing.MockedSources backtype.storm.Config]
                       java.util.Map]
             ^:static [completeTopology
                       [java.util.Map backtype.storm.generated.StormTopology]
                       java.util.Map]
             ^:static [withSimulatedTimeLocalCluster [backtype.storm.testing.TestJob] Object]
             ^:static [readTuples [java.util.Map String String] java.util.List]
             ^:static [readTuples [java.util.Map String] java.util.List]
             ^:static [eq [java.util.Collection java.util.Collection] boolean]
             ^:static [eq [java.util.Map java.util.Map] boolean]])
  )

(defn -completeTopology
  ([^Map clusterMap ^StormTopology topology
    ^MockedSources mockedSources ^Config stormConf
    ^Boolean cleanupState ^String topologyName]
     (complete-topology clusterMap topology :mock-sources (.getData mockedSources)
                     :storm-conf stormConf :cleanup-state cleanupState
                     :topology-name topologyName))
  ([^Map clusterMap ^StormTopology topology
    ^MockedSources mockedSources ^Config stormConf
    ^Boolean cleanupState]
     (-completeTopology clusterMap topology mockedSources stormConf cleanupState nil))
  ([^Map clusterMap ^StormTopology topology
    ^MockedSources mockedSources ^Config stormConf]
     (-completeTopology clusterMap topology mockedSources stormConf true))
  ([^Map clusterMap ^StormTopology topology
    ^MockedSources mockedSources]
     (-completeTopology clusterMap topology mockedSources (Config.)))
  ([^Map clusterMap ^StormTopology topology]
     (-completeTopology clusterMap topology (MockedSources.))))

(defn -withSimulatedTimeLocalCluster [^TestJob code]
  (with-simulated-time-local-cluster [cluster]
    (.run code cluster)))

(defn- find-tuples [^List fixed-tuples ^String stream]
  (let [ret (ArrayList.)]
    (doseq [fixed-tuple fixed-tuples]
      (if (= (.stream fixed-tuple) stream)
        (.add ret (.values fixed-tuple))))
    ret))

(defn -readTuples
  ([^Map result ^String componentId ^String streamId]
     (let [stream-result (.get result componentId)
           ret (if stream-result
                 (find-tuples stream-result streamId)
                 [])]
       ret))
  ([^Map result ^String componentId]
     (-readTuples result componentId Utils/DEFAULT_STREAM_ID)))

(defn- eq [^Object obj1 ^Object obj2]
  (let [obj1 (clojurify-structure obj1)
        obj2 (clojurify-structure obj2)]
    (ms= obj1 obj2)))

(defn -eq [^Collection coll1 ^Collection coll2]
     (eq coll1 coll2))

(defn -eq [^Map coll1 ^Map coll2]
  (eq coll1 coll2))
