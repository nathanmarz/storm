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
(ns org.apache.storm.supervisor-test
  (:use [clojure test])
  (:require [conjure.core])
  (:use [conjure core])
  (:require [clojure.contrib [string :as contrib-str]])
  (:require [clojure [string :as string] [set :as set]])
  (:import [org.apache.storm.testing TestWordCounter TestWordSpout TestGlobalCount TestAggregatesCounter TestPlannerSpout]
           [org.apache.storm.daemon.supervisor SupervisorUtils SyncProcessEvent SupervisorData]
           [java.util ArrayList Arrays HashMap]
           [org.apache.storm.testing.staticmocking MockedSupervisorUtils]
           [org.apache.storm.daemon.supervisor.workermanager DefaultWorkerManager])
  (:import [org.apache.storm.scheduler ISupervisor])
  (:import [org.apache.storm.utils Time Utils$UptimeComputer ConfigUtils])
  (:import [org.apache.storm.generated RebalanceOptions WorkerResources])
  (:import [org.apache.storm.testing.staticmocking MockedCluster])
  (:import [java.util UUID])
  (:import [org.apache.storm Thrift])
  (:import [org.mockito Mockito Matchers])
  (:import [org.mockito.exceptions.base MockitoAssertionError])
  (:import [java.io File])
  (:import [java.nio.file Files])
  (:import [org.apache.storm.utils Utils IPredicate])
  (:import [org.apache.storm.cluster StormClusterStateImpl ClusterStateContext ClusterUtils]
           [org.apache.storm.utils.staticmocking ConfigUtilsInstaller UtilsInstaller])
  (:import [java.nio.file.attribute FileAttribute])
  (:import [org.apache.storm.daemon StormCommon])
  (:use [org.apache.storm config testing util log converter])
  (:use [org.apache.storm.daemon common])
  (:require [org.apache.storm.daemon [worker :as worker] [local-supervisor :as local-supervisor]])
  (:use [conjure core])
  (:require [clojure.java.io :as io]))

(defn worker-assignment
  "Return [storm-id executors]"
  [cluster supervisor-id port]
  (let [state (:storm-cluster-state cluster)
        slot-assigns (for [storm-id (.assignments state nil)]
                        (let [executors (-> (clojurify-assignment (.assignmentInfo state storm-id nil))
                                        :executor->node+port
                                        (Utils/reverseMap)
                                        clojurify-structure
                                        (get [supervisor-id port] ))]
                          (when executors [storm-id executors])
                          ))
        pred (reify IPredicate (test [this x] (not-nil? x)))
        ret (Utils/findOne pred slot-assigns)]
    (when-not ret
      (throw (RuntimeException. "Could not find assignment for worker")))
    ret
    ))

(defn heartbeat-worker [supervisor port storm-id executors]
  (let [conf (.getConf supervisor)]
    (worker/do-heartbeat {:conf conf
                          :port port
                          :storm-id storm-id
                          :executors executors
                          :worker-id (find-worker-id conf port)})))

(defn heartbeat-workers [cluster supervisor-id ports]
  (let [sup (get-supervisor cluster supervisor-id)]
    (doseq [p ports]
      (let [[storm-id executors] (worker-assignment cluster supervisor-id p)]
        (heartbeat-worker sup p storm-id executors)
        ))))

;TODO: when translating this function, you should replace the map-val with a proper for loop HERE
(defn validate-launched-once [launched supervisor->ports storm-id]
  (let [counts (map count (vals launched))
        launched-supervisor->ports (apply merge-with set/union
                                          (for [[[s p] sids] launched
                                                :when (some #(= % storm-id) sids)]
                                            {s #{p}}))
        supervisor->ports (map-val set supervisor->ports)]
    (is (every? (partial = 1) counts))
    (is (= launched-supervisor->ports supervisor->ports))
    ))

(defmacro letlocals
  [& body]
  (let [[tobind lexpr] (split-at (dec (count body)) body)
        binded (vec (mapcat (fn [e]
                              (if (and (list? e) (= 'bind (first e)))
                                [(second e) (last e)]
                                ['_ e]
                                ))
                            tobind))]
    `(let ~binded
       ~(first lexpr))))

(deftest launches-assignment
  (with-simulated-time-local-cluster [cluster :supervisors 0
    :daemon-conf {ConfigUtils/NIMBUS_DO_NOT_REASSIGN true
                  SUPERVISOR-WORKER-START-TIMEOUT-SECS 5
                  SUPERVISOR-WORKER-TIMEOUT-SECS 15
                  SUPERVISOR-MONITOR-FREQUENCY-SECS 3}]
    (letlocals
      (bind topology (Thrift/buildTopology
                       {"1" (Thrift/prepareSpoutDetails
                              (TestPlannerSpout. true) (Integer. 4))}
                       {}))
      (bind sup1 (add-supervisor cluster :id "sup1" :ports [1 2 3 4]))
      (bind changed (capture-changed-workers
                      (submit-mocked-assignment
                        (:nimbus cluster)
                        (:storm-cluster-state cluster)
                        "test"
                        {TOPOLOGY-WORKERS 3}
                        topology
                        {1 "1"
                         2 "1"
                         3 "1"
                         4 "1"}
                        {[1 1] ["sup1" 1]
                         [2 2] ["sup1" 2]
                         [3 3] ["sup1" 3]
                         [4 4] ["sup1" 3]}
                        {["sup1" 1] [0.0 0.0 0.0]
                         ["sup1" 2] [0.0 0.0 0.0]
                         ["sup1" 3] [0.0 0.0 0.0]
                         })
                      ;; Instead of sleeping until topology is scheduled, rebalance topology so mk-assignments is called.
                      (.rebalance (:nimbus cluster) "test" (doto (RebalanceOptions.) (.set_wait_secs 0)))
                      (advance-cluster-time cluster 2)
                      (heartbeat-workers cluster "sup1" [1 2 3])
                      (advance-cluster-time cluster 10)))
      (bind storm-id (StormCommon/getStormId (:storm-cluster-state cluster) "test"))
      (is (empty? (:shutdown changed)))
      (validate-launched-once (:launched changed) {"sup1" [1 2 3]} storm-id)
      (bind changed (capture-changed-workers
                        (doseq [i (range 10)]
                          (heartbeat-workers cluster "sup1" [1 2 3])
                          (advance-cluster-time cluster 10))
                        ))
      (is (empty? (:shutdown changed)))
      (is (empty? (:launched changed)))
      (bind changed (capture-changed-workers
                      (heartbeat-workers cluster "sup1" [1 2])
                      (advance-cluster-time cluster 10)
                      ))
      (validate-launched-once (:launched changed) {"sup1" [3]} storm-id)
      (is (= {["sup1" 3] 1} (:shutdown changed)))
      )))

(deftest test-multiple-active-storms-multiple-supervisors
  (with-simulated-time-local-cluster [cluster :supervisors 0
    :daemon-conf {ConfigUtils/NIMBUS_DO_NOT_REASSIGN true
                  SUPERVISOR-WORKER-START-TIMEOUT-SECS 5
                  SUPERVISOR-WORKER-TIMEOUT-SECS 15
                  SUPERVISOR-MONITOR-FREQUENCY-SECS 3}]
    (letlocals
      (bind topology (Thrift/buildTopology
                       {"1" (Thrift/prepareSpoutDetails
                              (TestPlannerSpout. true) (Integer. 4))}
                       {}))
      (bind topology2 (Thrift/buildTopology
                       {"1" (Thrift/prepareSpoutDetails
                              (TestPlannerSpout. true) (Integer. 3))}
                       {}))
      (bind sup1 (add-supervisor cluster :id "sup1" :ports [1 2 3 4]))
      (bind sup2 (add-supervisor cluster :id "sup2" :ports [1 2]))
      (bind changed (capture-changed-workers
                      (submit-mocked-assignment
                        (:nimbus cluster)
                        (:storm-cluster-state cluster)
                        "test"
                        {TOPOLOGY-WORKERS 3 TOPOLOGY-MESSAGE-TIMEOUT-SECS 40}
                        topology
                        {1 "1"
                         2 "1"
                         3 "1"
                         4 "1"}
                        {[1 1] ["sup1" 1]
                         [2 2] ["sup1" 2]
                         [3 3] ["sup2" 1]
                         [4 4] ["sup2" 1]}
                        {["sup1" 1] [0.0 0.0 0.0]
                         ["sup1" 2] [0.0 0.0 0.0]
                         ["sup2" 1] [0.0 0.0 0.0]
                         })
                      ;; Instead of sleeping until topology is scheduled, rebalance topology so mk-assignments is called.
                      (.rebalance (:nimbus cluster) "test" (doto (RebalanceOptions.) (.set_wait_secs 0)))
                      (advance-cluster-time cluster 2)
                      (heartbeat-workers cluster "sup1" [1 2])
                      (heartbeat-workers cluster "sup2" [1])
                      ))
      (bind storm-id (StormCommon/getStormId (:storm-cluster-state cluster) "test"))
      (is (empty? (:shutdown changed)))
      (validate-launched-once (:launched changed) {"sup1" [1 2] "sup2" [1]} storm-id)
      (bind changed (capture-changed-workers
                      (submit-mocked-assignment
                        (:nimbus cluster)
                        (:storm-cluster-state cluster)
                        "test2"
                        {TOPOLOGY-WORKERS 2}
                        topology2
                        {1 "1"
                         2 "1"
                         3 "1"}
                        {[1 1] ["sup1" 3]
                         [2 2] ["sup1" 3]
                         [3 3] ["sup2" 2]}
                        {["sup1" 3] [0.0 0.0 0.0]
                         ["sup2" 2] [0.0 0.0 0.0]
                         })
                      ;; Instead of sleeping until topology is scheduled, rebalance topology so mk-assignments is called.
                      (.rebalance (:nimbus cluster) "test2" (doto (RebalanceOptions.) (.set_wait_secs 0)))
                      (advance-cluster-time cluster 2)
                      (heartbeat-workers cluster "sup1" [3])
                      (heartbeat-workers cluster "sup2" [2])
                      ))
      (bind storm-id2 (StormCommon/getStormId (:storm-cluster-state cluster) "test2"))
      (is (empty? (:shutdown changed)))
      (validate-launched-once (:launched changed) {"sup1" [3] "sup2" [2]} storm-id2)
      (bind changed (capture-changed-workers
        (.killTopology (:nimbus cluster) "test")
        (doseq [i (range 4)]
          (advance-cluster-time cluster 8)
          (heartbeat-workers cluster "sup1" [1 2 3])
          (heartbeat-workers cluster "sup2" [1 2])
          )))
      (is (empty? (:shutdown changed)))
      (is (empty? (:launched changed)))
      (bind changed (capture-changed-workers
        (advance-cluster-time cluster 12)
        ))
      (is (empty? (:launched changed)))
      (is (= {["sup1" 1] 1 ["sup1" 2] 1 ["sup2" 1] 1} (:shutdown changed)))
      (bind changed (capture-changed-workers
        (doseq [i (range 10)]
          (heartbeat-workers cluster "sup1" [3])
          (heartbeat-workers cluster "sup2" [2])
          (advance-cluster-time cluster 10)
          )))
      (is (empty? (:shutdown changed)))
      (is (empty? (:launched changed)))
      ;; TODO check that downloaded code is cleaned up only for the one storm
      )))

(defn get-heartbeat [cluster supervisor-id]
  (clojurify-supervisor-info (.supervisorInfo (:storm-cluster-state cluster) supervisor-id)))

(defn check-heartbeat [cluster supervisor-id within-secs]
  (let [hb (get-heartbeat cluster supervisor-id)
        time-secs (:time-secs hb)
        now (Time/currentTimeSecs)
        delta (- now time-secs)]
    (is (>= delta 0))
    (is (<= delta within-secs))
    ))

(deftest heartbeats-to-nimbus
  (with-simulated-time-local-cluster [cluster :supervisors 0
    :daemon-conf {SUPERVISOR-WORKER-START-TIMEOUT-SECS 15
                  SUPERVISOR-HEARTBEAT-FREQUENCY-SECS 3}]
    (letlocals
      (bind sup1 (add-supervisor cluster :id "sup" :ports [5 6 7]))
      (advance-cluster-time cluster 4)
      (bind hb (get-heartbeat cluster "sup"))
      (is (= #{5 6 7} (set (:meta hb))))
      (check-heartbeat cluster "sup" 3)
      (advance-cluster-time cluster 3)
      (check-heartbeat cluster "sup" 3)
      (advance-cluster-time cluster 3)
      (check-heartbeat cluster "sup" 3)
      (advance-cluster-time cluster 15)
      (check-heartbeat cluster "sup" 3)
      (bind topology (Thrift/buildTopology
                       {"1" (Thrift/prepareSpoutDetails
                              (TestPlannerSpout. true) (Integer. 4))}
                       {}))
      ;; prevent them from launching by capturing them
      (capture-changed-workers
       (submit-local-topology (:nimbus cluster) "test" {TOPOLOGY-WORKERS 2} topology)
       (advance-cluster-time cluster 3)
       (check-heartbeat cluster "sup" 3)
       (advance-cluster-time cluster 3)
       (check-heartbeat cluster "sup" 3)
       (advance-cluster-time cluster 3)
       (check-heartbeat cluster "sup" 3)
       (advance-cluster-time cluster 20)
       (check-heartbeat cluster "sup" 3))
      )))

(deftest test-worker-launch-command
  (testing "*.worker.childopts configuration"
    (let [mock-port 42
          mock-storm-id "fake-storm-id"
          mock-worker-id "fake-worker-id"
          mock-cp (str Utils/FILE_PATH_SEPARATOR "base" Utils/CLASS_PATH_SEPARATOR Utils/FILE_PATH_SEPARATOR "stormjar.jar")
          mock-sensitivity "S3"
          exp-args-fn (fn [opts topo-opts classpath]
                        (let [file-prefix (let [os (System/getProperty "os.name")]
                                            (if (.startsWith os "Windows") (str "file:///")
                                                    (str "")))
                              sequences (concat [(SupervisorUtils/javaCmd "java") "-cp" classpath
                                                (str "-Dlogfile.name=" "worker.log")
                                                "-Dstorm.home="
                                                (str "-Dworkers.artifacts=" "/tmp/workers-artifacts")
                                                (str "-Dstorm.id=" mock-storm-id)
                                                (str "-Dworker.id=" mock-worker-id)
                                                (str "-Dworker.port=" mock-port)
                                                (str "-Dstorm.log.dir=" (ConfigUtils/getLogDir))
                                                (str "-Dlog4j.configurationFile=" file-prefix Utils/FILE_PATH_SEPARATOR "log4j2" Utils/FILE_PATH_SEPARATOR "worker.xml")
                                                 "-DLog4jContextSelector=org.apache.logging.log4j.core.selector.BasicContextSelector"
                                                "org.apache.storm.LogWriter"]
                                         [(SupervisorUtils/javaCmd "java") "-server"]
                                         opts
                                         topo-opts
                                         ["-Djava.library.path="
                                          (str "-Dlogfile.name=" "worker.log")
                                          "-Dstorm.home="
                                          "-Dworkers.artifacts=/tmp/workers-artifacts"
                                          "-Dstorm.conf.file="
                                          "-Dstorm.options="
                                          (str "-Dstorm.log.dir=" (ConfigUtils/getLogDir))
                                          (str "-Djava.io.tmpdir=/tmp/workers" Utils/FILE_PATH_SEPARATOR mock-worker-id Utils/FILE_PATH_SEPARATOR "tmp")
                                          (str "-Dlogging.sensitivity=" mock-sensitivity)
                                          (str "-Dlog4j.configurationFile=" file-prefix Utils/FILE_PATH_SEPARATOR "log4j2" Utils/FILE_PATH_SEPARATOR "worker.xml")
                                          "-DLog4jContextSelector=org.apache.logging.log4j.core.selector.BasicContextSelector"
                                          (str "-Dstorm.id=" mock-storm-id)
                                          (str "-Dworker.id=" mock-worker-id)
                                          (str "-Dworker.port=" mock-port)
                                          "-cp" classpath
                                          "org.apache.storm.daemon.worker"
                                          mock-storm-id
                                          ""
                                          mock-port
                                          mock-worker-id])
                          ret (ArrayList.)]
                        (doseq [val sequences]
                          (.add ret (str val)))
                          ret))]
      (testing "testing *.worker.childopts as strings with extra spaces"
        (let [string-opts "-Dfoo=bar  -Xmx1024m"
              topo-string-opts "-Dkau=aux   -Xmx2048m"
              exp-args (exp-args-fn ["-Dfoo=bar" "-Xmx1024m"]
                                    ["-Dkau=aux" "-Xmx2048m"]
                                    mock-cp)
              mock-supervisor {STORM-CLUSTER-MODE :distributed
                                      WORKER-CHILDOPTS string-opts}
              mocked-supervisor-storm-conf {TOPOLOGY-WORKER-CHILDOPTS
                                            topo-string-opts}
              utils-spy (->>
                          (proxy [Utils] []
                            (addToClasspathImpl [classpath paths] mock-cp)
                            (launchProcessImpl [& _] nil))
                          Mockito/spy)
              cu-proxy (proxy [ConfigUtils] []
                          (supervisorStormDistRootImpl ([conf] nil)
                                                       ([conf storm-id] nil))
                          (readSupervisorStormConfImpl [conf storm-id] mocked-supervisor-storm-conf)
                          (setWorkerUserWSEImpl [conf worker-id user] nil)
                         (workerRootImpl [conf] "/tmp/workers")
                          (workerArtifactsRootImpl [conf] "/tmp/workers-artifacts"))
              worker-manager (proxy [DefaultWorkerManager] []
                               (jlp [stormRoot conf] ""))
              process-proxy (proxy [SyncProcessEvent] []
                              (writeLogMetadata [stormconf user workerId stormId port conf] nil)
                              (createBlobstoreLinks [conf stormId workerId] nil))]

          (with-open [_ (ConfigUtilsInstaller. cu-proxy)
                      _ (UtilsInstaller. utils-spy)]
                (.prepareWorker worker-manager mock-supervisor nil)
                (.launchDistributedWorker process-proxy worker-manager mock-supervisor nil
                                      "" mock-storm-id mock-port
                                      mock-worker-id
                                      (WorkerResources.) nil)
                (. (Mockito/verify utils-spy)
                   (launchProcessImpl (Matchers/eq exp-args)
                                      (Matchers/any)
                                      (Matchers/any)
                                      (Matchers/any)
                                      (Matchers/any))))))

      (testing "testing *.worker.childopts as list of strings, with spaces in values"
        (let [list-opts '("-Dopt1='this has a space in it'" "-Xmx1024m")
              topo-list-opts '("-Dopt2='val with spaces'" "-Xmx2048m")
              exp-args (exp-args-fn list-opts topo-list-opts mock-cp)
              mock-supervisor  {STORM-CLUSTER-MODE :distributed
                                      WORKER-CHILDOPTS list-opts}
              mocked-supervisor-storm-conf {TOPOLOGY-WORKER-CHILDOPTS
                                            topo-list-opts}
              cu-proxy (proxy [ConfigUtils] []
                          (supervisorStormDistRootImpl ([conf] nil)
                                                       ([conf storm-id] nil))
                          (readSupervisorStormConfImpl [conf storm-id] mocked-supervisor-storm-conf)
                          (setWorkerUserWSEImpl [conf worker-id user] nil)
                          (workerRootImpl [conf] "/tmp/workers")
                          (workerArtifactsRootImpl [conf] "/tmp/workers-artifacts"))
              utils-spy (->>
                          (proxy [Utils] []
                            (addToClasspathImpl [classpath paths] mock-cp)
                            (launchProcessImpl [& _] nil))
                          Mockito/spy)
              worker-manager (proxy [DefaultWorkerManager] []
                               (jlp [stormRoot conf] ""))
              process-proxy (proxy [SyncProcessEvent] []
                              (writeLogMetadata [stormconf user workerId stormId port conf] nil)
                              (createBlobstoreLinks [conf stormId workerId] nil))]
            (with-open [_ (ConfigUtilsInstaller. cu-proxy)
                        _ (UtilsInstaller. utils-spy)]
                  (.prepareWorker worker-manager mock-supervisor nil)
                  (.launchDistributedWorker process-proxy worker-manager mock-supervisor nil
                                            "" mock-storm-id
                                            mock-port
                                            mock-worker-id
                                            (WorkerResources.) nil)
                  (. (Mockito/verify utils-spy)
                     (launchProcessImpl (Matchers/eq exp-args)
                                        (Matchers/any)
                                        (Matchers/any)
                                        (Matchers/any)
                                        (Matchers/any))))))

      (testing "testing topology.classpath is added to classpath"
        (let [topo-cp (str Utils/FILE_PATH_SEPARATOR "any" Utils/FILE_PATH_SEPARATOR "path")
              exp-args (exp-args-fn [] [] (Utils/addToClasspath mock-cp [topo-cp]))
              mock-supervisor {STORM-CLUSTER-MODE :distributed}
              mocked-supervisor-storm-conf {TOPOLOGY-CLASSPATH topo-cp}
              cu-proxy (proxy [ConfigUtils] []
                          (supervisorStormDistRootImpl ([conf] nil)
                                                       ([conf storm-id] nil))
                          (readSupervisorStormConfImpl [conf storm-id] mocked-supervisor-storm-conf)
                          (setWorkerUserWSEImpl [conf worker-id user] nil)
                          (workerRootImpl [conf] "/tmp/workers")
                          (workerArtifactsRootImpl [conf] "/tmp/workers-artifacts"))
              utils-spy (->>
                          (proxy [Utils] []
                            (currentClasspathImpl []
                              (str Utils/FILE_PATH_SEPARATOR "base"))
                            (launchProcessImpl [& _] nil))
                          Mockito/spy)
              worker-manager (proxy [DefaultWorkerManager] []
                               (jlp [stormRoot conf] ""))
              process-proxy (proxy [SyncProcessEvent] []
                              (writeLogMetadata [stormconf user workerId stormId port conf] nil)
                              (createBlobstoreLinks [conf stormId workerId] nil))]
          (with-open [_ (ConfigUtilsInstaller. cu-proxy)
                      _ (UtilsInstaller. utils-spy)]
                  (.prepareWorker worker-manager mock-supervisor nil)
                  (.launchDistributedWorker process-proxy worker-manager mock-supervisor nil
                                               "" mock-storm-id
                                              mock-port
                                              mock-worker-id
                                              (WorkerResources.) nil)
                  (. (Mockito/verify utils-spy)
                     (launchProcessImpl (Matchers/eq exp-args)
                                        (Matchers/any)
                                        (Matchers/any)
                                        (Matchers/any)
                                        (Matchers/any))))))
      (testing "testing topology.environment is added to environment for worker launch"
        (let [topo-env {"THISVAR" "somevalue" "THATVAR" "someothervalue"}
              full-env (merge topo-env {"LD_LIBRARY_PATH" nil})
              exp-args (exp-args-fn [] [] mock-cp)
              mock-supervisor {STORM-CLUSTER-MODE :distributed}
              mocked-supervisor-storm-conf {TOPOLOGY-ENVIRONMENT topo-env}
              cu-proxy (proxy [ConfigUtils] []
                          (supervisorStormDistRootImpl ([conf] nil)
                                                       ([conf storm-id] nil))
                          (readSupervisorStormConfImpl [conf storm-id] mocked-supervisor-storm-conf)
                          (setWorkerUserWSEImpl [conf worker-id user] nil)
                          (workerRootImpl [conf] "/tmp/workers")
                          (workerArtifactsRootImpl [conf] "/tmp/workers-artifacts"))
              utils-spy (->>
                          (proxy [Utils] []
                            (currentClasspathImpl []
                              (str Utils/FILE_PATH_SEPARATOR "base"))
                            (launchProcessImpl [& _] nil))
                          Mockito/spy)
              worker-manager (proxy [DefaultWorkerManager] []
                               (jlp [stormRoot conf] nil))
              process-proxy (proxy [SyncProcessEvent] []
                              (writeLogMetadata [stormconf user workerId stormId port conf] nil)
                              (createBlobstoreLinks [conf stormId workerId] nil))]
          (with-open [_ (ConfigUtilsInstaller. cu-proxy)
                      _ (UtilsInstaller. utils-spy)]
            (.prepareWorker worker-manager mock-supervisor nil)
            (.launchDistributedWorker process-proxy worker-manager mock-supervisor nil
                                        "" mock-storm-id
                                        mock-port
                                        mock-worker-id
                                        (WorkerResources.) nil)
              (. (Mockito/verify utils-spy)
                 (launchProcessImpl (Matchers/any)
                                    (Matchers/eq full-env)
                                    (Matchers/any)
                                    (Matchers/any)
                                    (Matchers/any)))))))))

(deftest test-worker-launch-command-run-as-user
  (testing "*.worker.childopts configuration"
    (let [file-prefix (let [os (System/getProperty "os.name")]
                        (if (.startsWith os "Windows") (str "file:///")
                          (str "")))
          mock-port 42
          mock-storm-id "fake-storm-id"
          mock-worker-id "fake-worker-id"
          mock-sensitivity "S3"
          mock-cp "mock-classpath'quote-on-purpose"
          attrs (make-array FileAttribute 0)
          storm-local (.getCanonicalPath (.toFile (Files/createTempDirectory "storm-local" attrs)))
          worker-script (str storm-local Utils/FILE_PATH_SEPARATOR "workers" Utils/FILE_PATH_SEPARATOR mock-worker-id Utils/FILE_PATH_SEPARATOR "storm-worker-script.sh")
          exp-launch ["/bin/worker-launcher"
                      "me"
                      "worker"
                      (str storm-local Utils/FILE_PATH_SEPARATOR "workers" Utils/FILE_PATH_SEPARATOR mock-worker-id)
                      worker-script]
          exp-script-fn (fn [opts topo-opts]
                          (str "#!/bin/bash\n'export' 'LD_LIBRARY_PATH=';\n\nexec 'java'"
                               " '-cp' 'mock-classpath'\"'\"'quote-on-purpose'"
                               " '-Dlogfile.name=" "worker.log'"
                               " '-Dstorm.home='"
                               " '-Dworkers.artifacts=" (str storm-local "/workers-artifacts'")
                               " '-Dstorm.id=" mock-storm-id "'"
                               " '-Dworker.id=" mock-worker-id "'"
                               " '-Dworker.port=" mock-port "'"
                               " '-Dstorm.log.dir=" (ConfigUtils/getLogDir) "'"
                               " '-Dlog4j.configurationFile=" (str file-prefix Utils/FILE_PATH_SEPARATOR "log4j2" Utils/FILE_PATH_SEPARATOR "worker.xml'")
                               " '-DLog4jContextSelector=org.apache.logging.log4j.core.selector.BasicContextSelector'"
                               " 'org.apache.storm.LogWriter'"
                               " 'java' '-server'"
                               " " (Utils/shellCmd opts)
                               " " (Utils/shellCmd topo-opts)
                               " '-Djava.library.path='"
                               " '-Dlogfile.name=" "worker.log'"
                               " '-Dstorm.home='"
                               " '-Dworkers.artifacts=" (str storm-local "/workers-artifacts'")
                               " '-Dstorm.conf.file='"
                               " '-Dstorm.options='"
                               " '-Dstorm.log.dir=" (ConfigUtils/getLogDir) "'"
                               " '-Djava.io.tmpdir=" (str  storm-local "/workers/" mock-worker-id "/tmp'")
                               " '-Dlogging.sensitivity=" mock-sensitivity "'"
                               " '-Dlog4j.configurationFile=" (str file-prefix Utils/FILE_PATH_SEPARATOR "log4j2" Utils/FILE_PATH_SEPARATOR "worker.xml'")
                               " '-DLog4jContextSelector=org.apache.logging.log4j.core.selector.BasicContextSelector'"
                               " '-Dstorm.id=" mock-storm-id "'"
                               " '-Dworker.id=" mock-worker-id "'"
                               " '-Dworker.port=" mock-port "'"
                               " '-cp' 'mock-classpath'\"'\"'quote-on-purpose'"
                               " 'org.apache.storm.daemon.worker'"
                               " '" mock-storm-id "'"
                               " '""'"
                               " '" mock-port "'"
                               " '" mock-worker-id "';"))]
      (try
        (testing "testing *.worker.childopts as strings with extra spaces"
          (let [string-opts "-Dfoo=bar  -Xmx1024m"
                topo-string-opts "-Dkau=aux   -Xmx2048m"
                exp-script (exp-script-fn ["-Dfoo=bar" "-Xmx1024m"]
                                          ["-Dkau=aux" "-Xmx2048m"])
                _ (.mkdirs (io/file storm-local "workers" mock-worker-id))
                mock-supervisor {STORM-CLUSTER-MODE :distributed
                                        STORM-LOCAL-DIR storm-local
                                        STORM-WORKERS-ARTIFACTS-DIR (str storm-local "/workers-artifacts")
                                        SUPERVISOR-RUN-WORKER-AS-USER true
                                        WORKER-CHILDOPTS string-opts}
                mocked-supervisor-storm-conf {TOPOLOGY-WORKER-CHILDOPTS
                                              topo-string-opts
                                              TOPOLOGY-SUBMITTER-USER "me"}
                cu-proxy (proxy [ConfigUtils] []
                          (supervisorStormDistRootImpl ([conf] nil)
                                                       ([conf storm-id] nil))
                          (readSupervisorStormConfImpl [conf storm-id] mocked-supervisor-storm-conf)
                          (setWorkerUserWSEImpl [conf worker-id user] nil))
                utils-spy (->>
                            (proxy [Utils] []
                              (addToClasspathImpl [classpath paths] mock-cp)
                              (launchProcessImpl [& _] nil))
                            Mockito/spy)
                supervisor-utils (Mockito/mock SupervisorUtils)
                worker-manager (proxy [DefaultWorkerManager] []
                                 (jlp [stormRoot conf] ""))
                process-proxy (proxy [SyncProcessEvent] []
                                (writeLogMetadata [stormconf user workerId stormId port conf] nil))]
            (with-open [_ (ConfigUtilsInstaller. cu-proxy)
                        _ (UtilsInstaller. utils-spy)
                        _ (MockedSupervisorUtils. supervisor-utils)]
              (. (Mockito/when (.javaCmdImpl supervisor-utils (Mockito/any))) (thenReturn (str "java")))
              (.prepareWorker worker-manager mock-supervisor nil)
              (.launchDistributedWorker process-proxy worker-manager mock-supervisor nil
                                          "" mock-storm-id
                                          mock-port
                                          mock-worker-id
                                          (WorkerResources.) nil)
                (. (Mockito/verify utils-spy)
                   (launchProcessImpl (Matchers/eq exp-launch)
                                      (Matchers/any)
                                      (Matchers/any)
                                      (Matchers/any)
                                      (Matchers/any))))
            (is (= (slurp worker-script) exp-script))
            ))
        (finally (Utils/forceDelete storm-local)))
      (.mkdirs (io/file storm-local "workers" mock-worker-id))
      (try
        (testing "testing *.worker.childopts as list of strings, with spaces in values"
          (let [list-opts '("-Dopt1='this has a space in it'" "-Xmx1024m")
                topo-list-opts '("-Dopt2='val with spaces'" "-Xmx2048m")
                exp-script (exp-script-fn list-opts topo-list-opts)
                mock-supervisor {STORM-CLUSTER-MODE :distributed
                                        STORM-LOCAL-DIR storm-local
                                        STORM-WORKERS-ARTIFACTS-DIR (str storm-local "/workers-artifacts")
                                        SUPERVISOR-RUN-WORKER-AS-USER true
                                        WORKER-CHILDOPTS list-opts}
                mocked-supervisor-storm-conf {TOPOLOGY-WORKER-CHILDOPTS
                                              topo-list-opts
                                              TOPOLOGY-SUBMITTER-USER "me"}
                cu-proxy (proxy [ConfigUtils] []
                          (supervisorStormDistRootImpl ([conf] nil)
                                                       ([conf storm-id] nil))
                          (readSupervisorStormConfImpl [conf storm-id] mocked-supervisor-storm-conf)
                          (setWorkerUserWSEImpl [conf worker-id user] nil))
                utils-spy (->>
                            (proxy [Utils] []
                              (addToClasspathImpl [classpath paths] mock-cp)
                              (launchProcessImpl [& _] nil))
                            Mockito/spy)
                supervisor-utils (Mockito/mock SupervisorUtils)
                worker-manager (proxy [DefaultWorkerManager] []
                                 (jlp [stormRoot conf] ""))
                process-proxy (proxy [SyncProcessEvent] []
                                (writeLogMetadata [stormconf user workerId stormId port conf] nil))]
            (with-open [_ (ConfigUtilsInstaller. cu-proxy)
                        _ (UtilsInstaller. utils-spy)
                        _ (MockedSupervisorUtils. supervisor-utils)]
              (. (Mockito/when (.javaCmdImpl supervisor-utils (Mockito/any))) (thenReturn (str "java")))
              (.prepareWorker worker-manager mock-supervisor nil)
              (.launchDistributedWorker process-proxy worker-manager mock-supervisor nil
                                          "" mock-storm-id
                                          mock-port
                                          mock-worker-id
                                          (WorkerResources.) nil)
                (. (Mockito/verify utils-spy)
                 (launchProcessImpl (Matchers/eq exp-launch)
                                    (Matchers/any)
                                    (Matchers/any)
                                    (Matchers/any)
                                    (Matchers/any))))
            (is (= (slurp worker-script) exp-script))
            ))
        (finally (Utils/forceDelete storm-local))))))

(deftest test-workers-go-bananas
  ;; test that multiple workers are started for a port, and test that
  ;; supervisor shuts down propertly (doesn't shutdown the most
  ;; recently launched one, checks heartbeats correctly, etc.)
  )

(deftest downloads-code
  )

(deftest test-stateless
  )

(deftest cleans-up-on-unassign
  ;; TODO just do reassign, and check that cleans up worker states after killing but doesn't get rid of downloaded code
  )

(deftest test-supervisor-data-acls
  (testing "supervisor-data uses correct ACLs"
    (let [scheme "digest"
          digest "storm:thisisapoorpassword"
          auth-conf {STORM-ZOOKEEPER-AUTH-SCHEME scheme
                     STORM-ZOOKEEPER-AUTH-PAYLOAD digest
                     STORM-SUPERVISOR-WORKER-MANAGER-PLUGIN "org.apache.storm.daemon.supervisor.workermanager.DefaultWorkerManager"}
          expected-acls (SupervisorUtils/supervisorZkAcls)
          fake-isupervisor (reify ISupervisor
                             (getSupervisorId [this] nil)
                             (getAssignmentId [this] nil))
          fake-cu (proxy [ConfigUtils] []
                    (supervisorStateImpl [conf] nil)
                    (supervisorLocalDirImpl [conf] nil))
          fake-utils (proxy [Utils] []
                       (localHostnameImpl [] nil)
                       (makeUptimeComputer [] (proxy [Utils$UptimeComputer] []
                                                (upTime [] 0))))
          cluster-utils (Mockito/mock ClusterUtils)]
      (with-open [_ (ConfigUtilsInstaller. fake-cu)
                  _ (UtilsInstaller. fake-utils)
                  mocked-cluster (MockedCluster. cluster-utils)]
          (SupervisorData. auth-conf nil fake-isupervisor)
          (.mkStormClusterStateImpl (Mockito/verify cluster-utils (Mockito/times 1)) (Mockito/any) (Mockito/eq expected-acls) (Mockito/any))))))

  (deftest test-write-log-metadata
    (testing "supervisor writes correct data to logs metadata file"
      (let [exp-owner "alice"
            exp-worker-id "42"
            exp-storm-id "0123456789"
            exp-port 4242
            exp-logs-users ["bob" "charlie" "daryl"]
            exp-logs-groups ["read-only-group" "special-group"]
            storm-conf {TOPOLOGY-SUBMITTER-USER "alice"
                        TOPOLOGY-USERS ["charlie" "bob"]
                        TOPOLOGY-GROUPS ["special-group"]
                        LOGS-GROUPS ["read-only-group"]
                        LOGS-USERS ["daryl"]}
            exp-data {TOPOLOGY-SUBMITTER-USER exp-owner
                      "worker-id" exp-worker-id
                      LOGS-USERS exp-logs-users
                      LOGS-GROUPS exp-logs-groups}
            conf {}
            process-proxy (->> (proxy [SyncProcessEvent] []
                            (writeLogMetadataToYamlFile [stormId  port data conf] nil))
                            Mockito/spy)]
          (.writeLogMetadata process-proxy storm-conf exp-owner exp-worker-id
            exp-storm-id  exp-port conf)
        (.writeLogMetadataToYamlFile (Mockito/verify process-proxy (Mockito/times 1)) (Mockito/eq exp-storm-id) (Mockito/eq exp-port) (Mockito/any) (Mockito/eq conf)))))

  (deftest test-worker-launcher-requires-user
    (testing "worker-launcher throws on blank user"
      (let [utils-proxy (proxy [Utils] []
                          (launchProcessImpl [& _] nil))]
        (with-open [_ (UtilsInstaller. utils-proxy)]
          (is (try
                (SupervisorUtils/processLauncher {} nil nil (ArrayList.) {} nil nil nil)
                false
                (catch Throwable t
                  (and (re-matches #"(?i).*user cannot be blank.*" (.getMessage t))
                       (Utils/exceptionCauseIsInstanceOf java.lang.IllegalArgumentException t)))))))))

  (defn found? [sub-str input-str]
    (if (string? input-str)
      (contrib-str/substring? sub-str (str input-str))
      (boolean (some #(contrib-str/substring? sub-str %) input-str))))

  (defn not-found? [sub-str input-str]
    (complement (found? sub-str input-str)))

  (deftest test-substitute-childopts-happy-path-string
    (testing "worker-launcher replaces ids in childopts"
      (let [worker-id "w-01"
            topology-id "s-01"
            port 9999
            mem-onheap (int 512)
            childopts "-Xloggc:/home/y/lib/storm/current/logs/gc.worker-%ID%-%TOPOLOGY-ID%-%WORKER-ID%-%WORKER-PORT%.log -Xms256m -Xmx%HEAP-MEM%m"
            expected-childopts '("-Xloggc:/home/y/lib/storm/current/logs/gc.worker-9999-s-01-w-01-9999.log" "-Xms256m" "-Xmx512m")
            worker-manager (DefaultWorkerManager.)
            childopts-with-ids (vec (.substituteChildopts worker-manager childopts worker-id topology-id port mem-onheap))]
        (is (= expected-childopts childopts-with-ids)))))

  (deftest test-substitute-childopts-happy-path-list
    (testing "worker-launcher replaces ids in childopts"
      (let [worker-id "w-01"
            topology-id "s-01"
            port 9999
            mem-onheap (int 512)
            childopts '("-Xloggc:/home/y/lib/storm/current/logs/gc.worker-%ID%-%TOPOLOGY-ID%-%WORKER-ID%-%WORKER-PORT%.log" "-Xms256m" "-Xmx%HEAP-MEM%m")
            expected-childopts '("-Xloggc:/home/y/lib/storm/current/logs/gc.worker-9999-s-01-w-01-9999.log" "-Xms256m" "-Xmx512m")
            worker-manager (DefaultWorkerManager.)
            childopts-with-ids (vec (.substituteChildopts worker-manager childopts worker-id topology-id port mem-onheap))]
        (is (= expected-childopts childopts-with-ids)))))

  (deftest test-substitute-childopts-happy-path-list-arraylist
    (testing "worker-launcher replaces ids in childopts"
      (let [worker-id "w-01"
            topology-id "s-01"
            port 9999
            mem-onheap (int 512)
            childopts '["-Xloggc:/home/y/lib/storm/current/logs/gc.worker-%ID%-%TOPOLOGY-ID%-%WORKER-ID%-%WORKER-PORT%.log" "-Xms256m" "-Xmx%HEAP-MEM%m"]
            expected-childopts '("-Xloggc:/home/y/lib/storm/current/logs/gc.worker-9999-s-01-w-01-9999.log" "-Xms256m" "-Xmx512m")
            worker-manager (DefaultWorkerManager.)
            childopts-with-ids (vec (.substituteChildopts worker-manager childopts worker-id topology-id port mem-onheap))]
        (is (= expected-childopts childopts-with-ids)))))

  (deftest test-substitute-childopts-topology-id-alone
    (testing "worker-launcher replaces ids in childopts"
      (let [worker-id "w-01"
            topology-id "s-01"
            port 9999
            mem-onheap (int 512)
            childopts "-Xloggc:/home/y/lib/storm/current/logs/gc.worker-%TOPOLOGY-ID%.log"
            expected-childopts '("-Xloggc:/home/y/lib/storm/current/logs/gc.worker-s-01.log")
            worker-manager (DefaultWorkerManager.)
            childopts-with-ids (vec (.substituteChildopts worker-manager childopts worker-id topology-id port mem-onheap))]
        (is (= expected-childopts childopts-with-ids)))))

  (deftest test-substitute-childopts-no-keys
    (testing "worker-launcher has no ids to replace in childopts"
      (let [worker-id "w-01"
            topology-id "s-01"
            port 9999
            mem-onheap (int 512)
            childopts "-Xloggc:/home/y/lib/storm/current/logs/gc.worker.log"
            expected-childopts '("-Xloggc:/home/y/lib/storm/current/logs/gc.worker.log")
            worker-manager (DefaultWorkerManager.)
            childopts-with-ids (vec (.substituteChildopts worker-manager childopts worker-id topology-id port mem-onheap))]
        (is (= expected-childopts childopts-with-ids)))))

  (deftest test-substitute-childopts-nil-childopts
    (testing "worker-launcher has nil childopts"
      (let [worker-id "w-01"
            topology-id "s-01"
            port 9999
            mem-onheap (int 512)
            childopts nil
            expected-childopts '[]
            worker-manager (DefaultWorkerManager.)
            childopts-with-ids (vec (.substituteChildopts worker-manager childopts worker-id topology-id port mem-onheap))]
        (is (= expected-childopts childopts-with-ids)))))

  (deftest test-substitute-childopts-nil-ids
    (testing "worker-launcher has nil ids"
      (let [worker-id ""
            topology-id "s-01"
            port 9999
            mem-onheap (int 512)
            childopts "-Xloggc:/home/y/lib/storm/current/logs/gc.worker-%ID%-%TOPOLOGY-ID%-%WORKER-ID%-%WORKER-PORT%.log"
            expected-childopts '("-Xloggc:/home/y/lib/storm/current/logs/gc.worker-9999-s-01--9999.log")
            worker-manager (DefaultWorkerManager.)
            childopts-with-ids (vec (.substituteChildopts worker-manager childopts worker-id topology-id port mem-onheap))]
        (is (= expected-childopts childopts-with-ids)))))

  (deftest test-retry-read-assignments
    (with-simulated-time-local-cluster [cluster
                                        :supervisors 0
                                        :ports-per-supervisor 2
                                        :daemon-conf {ConfigUtils/NIMBUS_DO_NOT_REASSIGN true
                                                      NIMBUS-MONITOR-FREQ-SECS 10
                                                      TOPOLOGY-MESSAGE-TIMEOUT-SECS 30
                                                      TOPOLOGY-ACKER-EXECUTORS 0}]
      (letlocals
        (bind sup1 (add-supervisor cluster :id "sup1" :ports [1 2 3 4]))
        (bind topology1 (Thrift/buildTopology
                          {"1" (Thrift/prepareSpoutDetails
                                 (TestPlannerSpout. true) (Integer. 2))}
                          {}))
        (bind topology2 (Thrift/buildTopology
                          {"1" (Thrift/prepareSpoutDetails
                                 (TestPlannerSpout. true) (Integer. 2))}
                          {}))
        (bind state (:storm-cluster-state cluster))
        (bind changed (capture-changed-workers
                        (submit-mocked-assignment
                          (:nimbus cluster)
                          (:storm-cluster-state cluster)
                          "topology1"
                          {TOPOLOGY-WORKERS 2}
                          topology1
                          {1 "1"
                           2 "1"}
                          {[1 1] ["sup1" 1]
                           [2 2] ["sup1" 2]}
                          {["sup1" 1] [0.0 0.0 0.0]
                           ["sup1" 2] [0.0 0.0 0.0]
                           })
                        (submit-mocked-assignment
                          (:nimbus cluster)
                          (:storm-cluster-state cluster)
                          "topology2"
                          {TOPOLOGY-WORKERS 2}
                          topology2
                          {1 "1"
                           2 "1"}
                          {[1 1] ["sup1" 1]
                           [2 2] ["sup1" 2]}
                          {["sup1" 1] [0.0 0.0 0.0]
                           ["sup1" 2] [0.0 0.0 0.0]
                           })
                        ;; Instead of sleeping until topology is scheduled, rebalance topology so mk-assignments is called.
                        (.rebalance (:nimbus cluster) "topology1" (doto (RebalanceOptions.) (.set_wait_secs 0)))
                        ))
        (is (empty? (:launched changed)))
        (bind options (RebalanceOptions.))
        (.set_wait_secs options 0)
        (bind changed (capture-changed-workers
                        (.rebalance (:nimbus cluster) "topology2" options)
                        (advance-cluster-time cluster 10)
                        (heartbeat-workers cluster "sup1" [1 2 3 4])
                        (advance-cluster-time cluster 10)
                        ))
        (validate-launched-once (:launched changed)
          {"sup1" [1 2]}
          (StormCommon/getStormId (:storm-cluster-state cluster) "topology1"))
        (validate-launched-once (:launched changed)
          {"sup1" [3 4]}
          (StormCommon/getStormId (:storm-cluster-state cluster) "topology2"))
        )))
