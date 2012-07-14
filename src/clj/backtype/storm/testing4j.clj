(ns backtype.storm.testing4j
  (:import [java.util Map List Collection ArrayList])
  (:import [backtype.storm Config])
  (:import [backtype.storm.generated StormTopology])
  (:import [backtype.storm.daemon nimbus])
  (:import [backtype.storm.testing TestJob MockedSources TrackedTopology Cluster
            MkClusterParam CompleteTopologyParam])
  (:import [backtype.storm.utils Utils])
  (:use [backtype.storm testing util log])
  (:gen-class
   :name backtype.storm.Testing
   :methods [^:static [completeTopology
                       [backtype.storm.testing.Cluster  backtype.storm.generated.StormTopology
                        backtype.storm.testing.CompleteTopologyParam]
                       java.util.Map]
             ^:static [completeTopology
                       [backtype.storm.testing.Cluster backtype.storm.generated.StormTopology]
                       java.util.Map]
             ^:static [withSimulatedTimeLocalCluster [backtype.storm.testing.TestJob] void]
             ^:static [withSimulatedTimeLocalCluster [backtype.storm.testing.MkClusterParam backtype.storm.testing.TestJob] void]
             ^:static [withTrackedCluster [backtype.storm.testing.TestJob] void]
             ^:static [withTrackedCluster [backtype.storm.testing.MkClusterParam backtype.storm.testing.TestJob] void]
             ^:static [readTuples [java.util.Map String String] java.util.List]
             ^:static [readTuples [java.util.Map String] java.util.List]
             ^:static [submitLocalTopology [Object String
                                            backtype.storm.Config backtype.storm.generated.StormTopology]
                       void]
             ^:static [mkTrackedTopology [backtype.storm.testing.Cluster backtype.storm.generated.StormTopology] backtype.storm.testing.TrackedTopology]
             ^:static [trackedWait [backtype.storm.testing.TrackedTopology] void]
             ^:static [trackedWait [backtype.storm.testing.TrackedTopology Integer] void]
             ^:static [advanceClusterTime [backtype.storm.testing.Cluster Integer Integer] void]
             ^:static [advanceClusterTime [backtype.storm.testing.Cluster Integer] void]
             ^:static [eq [java.util.Collection java.util.Collection] boolean]
             ^:static [eq [java.util.Map java.util.Map] boolean]])
  )

(defn -completeTopology
  ([^Cluster cluster ^StormTopology topology ^CompleteTopologyParam completeTopologyParam]
     (let [mocked-sources (or (-> completeTopologyParam .getMockedSources .getData) {})
           storm-conf (or (.getStormConf completeTopologyParam) {})
           cleanup-state (or (.getCleanupState completeTopologyParam) true)
           topology-name (or (.getTopologyName completeTopologyParam))]
       (complete-topology cluster topology
                          :mock-sources mocked-sources
                          :storm-conf storm-conf
                          :cleanup-state cleanup-state
                          :topology-name topology-name)))
  ([^Cluster cluster ^StormTopology topology]
     (-completeTopology cluster topology (CompleteTopologyParam.))))

(defn -withSimulatedTimeLocalCluster
  ([^MkClusterParam mkClusterParam ^TestJob code]
     (let [supervisors (or (.getSupervisors mkClusterParam) 2)
           ports-per-supervisor (or (.getPortsPerSupervisor mkClusterParam) 3)
           daemon-conf (or (.getDaemonConf mkClusterParam) {})]
       (with-simulated-time-local-cluster [cluster :supervisors supervisors
                                           :ports-per-supervisor ports-per-supervisor
                                           :daemon-conf daemon-conf]
         (let [cluster (Cluster. cluster)]
           (.run code cluster)))))
  ([^TestJob code]
     (-withSimulatedTimeLocalCluster (MkClusterParam.) code)))

(defn -withTrackedCluster
  ([^MkClusterParam mkClusterParam ^TestJob code]
     (let [supervisors (or (.getSupervisors mkClusterParam) 2)
           ports-per-supervisor (or (.getPortsPerSupervisor mkClusterParam) 3)
           daemon-conf (or (.getDaemonConf mkClusterParam) {})]
       (with-tracked-cluster [cluster :supervisors supervisors
                              :ports-per-supervisor ports-per-supervisor
                              :daemon-conf daemon-conf]
         (let [cluster (Cluster. cluster)]
           (.run code cluster)))))
  ([^TestJob code]
     (-withTrackedCluster (MkClusterParam.) code)))

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

(defn -submitLocalTopology [^Object nimbus ^String topologyName ^Config config ^StormTopology topology]
  (submit-local-topology nimbus topologyName config topology))



(defn -mkTrackedTopology [^Cluster trackedCluster ^StormTopology topology]
  (-> (mk-tracked-topology trackedCluster topology)
      (TrackedTopology.)))

(defn -trackedWait
  ([^TrackedTopology trackedTopology ^Integer amt]
     (tracked-wait trackedTopology amt))
  ([^TrackedTopology trackedTopology]
     (-trackedWait trackedTopology 1)))

(defn -advanceClusterTime
  ([^Cluster cluster ^Integer secs ^Integer step]
     (advance-cluster-time cluster secs step))
  ([^Cluster cluster ^Integer secs]
      (-advanceClusterTime cluster secs 1)))

(defn- eq [^Object obj1 ^Object obj2]
  (let [obj1 (clojurify-structure obj1)
        obj2 (clojurify-structure obj2)]
    (ms= obj1 obj2)))

(defn -eq [^Collection coll1 ^Collection coll2]
     (eq coll1 coll2))

(defn -eq [^Map coll1 ^Map coll2]
  (eq coll1 coll2))
