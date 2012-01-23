(ns backtype.storm.testing
  (:require [backtype.storm.daemon
             [nimbus :as nimbus]
             [supervisor :as supervisor]
             [common :as common]
             [worker :as worker]
             [task :as task]])
  (:require [backtype.storm [process-simulator :as psim]])
  (:import [org.apache.commons.io FileUtils])
  (:import [java.io File])
  (:import [java.util.concurrent.atomic AtomicInteger])
  (:import [java.util.concurrent ConcurrentHashMap])
  (:import [backtype.storm.utils Time Utils RegisteredGlobalState])
  (:import [backtype.storm.tuple Fields])
  (:import [backtype.storm.generated GlobalStreamId Bolt])
  (:import [backtype.storm.testing FeederSpout FixedTupleSpout FixedTuple TupleCaptureBolt
            SpoutTracker BoltTracker])
  (:require [backtype.storm [zookeeper :as zk]])
  (:require [backtype.storm.messaging.loader :as msg-loader])
  (:require [backtype.storm.daemon.acker :as acker])
  (:use [clojure.contrib.def :only [defnk]])
  (:use [clojure.contrib.seq :only [find-first]])
  (:use [backtype.storm cluster util thrift config log]))

(defn feeder-spout [fields]
  (FeederSpout. (Fields. fields)))

(defn local-temp-path []
  (str (System/getProperty "java.io.tmpdir") "/" (uuid)))

(defn delete-all [paths]
  (dorun
    (for [t paths]
      (if (.exists (File. t))
        (FileUtils/forceDelete (File. t))
        ))))

(defn with-local-tmp* [n afn]
  (let [tmp-paths (take n (repeatedly local-temp-path))]
    (try (apply afn tmp-paths)
         (finally
          (delete-all tmp-paths)))))

(defmacro with-local-tmp
  [[& tmp-syms] & body]
  `(with-local-tmp* ~(count tmp-syms)
     (fn [~@tmp-syms] ~@body)))

(defn start-simulating-time! []
  (Time/startSimulating))

(defn stop-simulating-time! []
  (Time/stopSimulating))

(defn simulated-time-call [f]
  (do (start-simulating-time!)
      (let [ret (f)]
        (stop-simulating-time!)
        ret)))

(defmacro with-simulated-time [& body]
  `(simulated-time-call (fn [] ~@body)))

(defn advance-time-ms! [ms]
  (Time/advanceTime ms))

(defn advance-time-secs! [secs]
  (advance-time-ms! (* (long secs) 1000)))

(defnk add-supervisor [cluster-map :ports 2 :conf {} :id nil]
  (let [tmp-dir (local-temp-path)
        port-ids (if (sequential? ports) ports (doall (repeatedly ports (:port-counter cluster-map))))
        supervisor-conf (merge (:daemon-conf cluster-map)
                               conf
                               {STORM-LOCAL-DIR tmp-dir
                                SUPERVISOR-SLOTS-PORTS port-ids
                               })
        id-fn (if id (fn [] id) supervisor/generate-supervisor-id)
        daemon (with-var-roots [supervisor/generate-supervisor-id id-fn] (supervisor/mk-supervisor supervisor-conf (:shared-context cluster-map)))]
    (swap! (:supervisors cluster-map) conj daemon)
    (swap! (:tmp-dirs cluster-map) conj tmp-dir)
    daemon
    ))

(defn mk-shared-context [conf]
  (if (and (= (conf STORM-CLUSTER-MODE) "local")
           (not (conf STORM-LOCAL-MODE-ZMQ)))
    (msg-loader/mk-local-context)
    ))

;; returns map containing cluster info
;; local dir is always overridden in maps
;; can customize the supervisors (except for ports) by passing in map for :supervisors parameter
;; if need to customize amt of ports more, can use add-supervisor calls afterwards
(defnk mk-local-storm-cluster [:supervisors 2 :ports-per-supervisor 3 :daemon-conf {}]
  (let [zk-port (available-port 2181)
        daemon-conf (merge (read-storm-config)
                           {TOPOLOGY-SKIP-MISSING-KRYO-REGISTRATIONS true
                            ZMQ-LINGER-MILLIS 0
                            }
                           daemon-conf
                           {STORM-CLUSTER-MODE "local"
                            STORM-ZOOKEEPER-PORT zk-port})
        nimbus-tmp (local-temp-path)
        zk-tmp (local-temp-path)
        zk-handle (zk/mk-inprocess-zookeeper zk-tmp zk-port)
        port-counter (mk-counter)
        nimbus (nimbus/service-handler
                (assoc daemon-conf STORM-LOCAL-DIR nimbus-tmp))
        context (mk-shared-context daemon-conf)
        cluster-map {:nimbus nimbus
                     :port-counter port-counter
                     :daemon-conf daemon-conf
                     :supervisors (atom [])
                     :state (mk-distributed-cluster-state daemon-conf)
                     :storm-cluster-state (mk-storm-cluster-state daemon-conf)
                     :tmp-dirs (atom [nimbus-tmp zk-tmp])
                     :zookeeper zk-handle
                     :shared-context context}
        supervisor-confs (if (sequential? supervisors)
                           supervisors
                           (repeat supervisors {}))]
    (doseq [sc supervisor-confs]
      (add-supervisor cluster-map :ports ports-per-supervisor :conf sc))
    cluster-map
    ))

(defn get-supervisor [cluster-map supervisor-id]
  (let [finder-fn #(= (.get-id %) supervisor-id)]
    (find-first finder-fn @(:supervisors cluster-map))
    ))

(defn kill-supervisor [cluster-map supervisor-id]
  (let [finder-fn #(= (.get-id %) supervisor-id)
        supervisors @(:supervisors cluster-map)
        sup (find-first finder-fn
                        supervisors)]
    ;; tmp-dir will be taken care of by shutdown
    (reset! (:supervisors cluster-map) (remove-first finder-fn supervisors))
    (.shutdown sup)
    ))

(defn kill-local-storm-cluster [cluster-map]
  (.shutdown (:nimbus cluster-map))
  (.close (:state cluster-map))
  (.disconnect (:storm-cluster-state cluster-map))
  (doseq [s @(:supervisors cluster-map)]
    (.shutdown-all-workers s)
    ;; race condition here? will it launch the workers again?
    (supervisor/kill-supervisor s))
  (psim/kill-all-processes)
  (log-message "Shutting down in process zookeeper")
  (zk/shutdown-inprocess-zookeeper (:zookeeper cluster-map))
  (log-message "Done shutting down in process zookeeper")
  (doseq [t @(:tmp-dirs cluster-map)]
    (log-message "Deleting temporary path " t)
    (rmr t)
    ))

(defn wait-until-cluster-waiting
  "Wait until the cluster is idle. Should be used with time simulation."
  [cluster-map]
  ;; wait until all workers, supervisors, and nimbus is waiting
  (let [supervisors @(:supervisors cluster-map)
        workers (filter (partial satisfies? common/DaemonCommon) (psim/all-processes))
        daemons (concat
                  [(:nimbus cluster-map)]
                  supervisors
                  workers) ; because a worker may already be dead
        ]
    (while (not (every? (memfn waiting?) daemons))
      (Thread/sleep 10)
      )))

(defn advance-cluster-time
  ([cluster-map secs increment-secs]
    (loop [left secs]
      (when (> left 0)
        (let [diff (min left increment-secs)]
          (advance-time-secs! diff)
          (wait-until-cluster-waiting cluster-map)
          (recur (- left diff))
          ))))
  ([cluster-map secs]
    (advance-cluster-time cluster-map secs 1)
    ))

(defn with-local-cluster* [f & cluster-args]
  (let [cluster (apply mk-local-storm-cluster cluster-args)]
    (try (f cluster)
         (catch Throwable t
           (log-error t "Error in cluster"))
         (finally
          (kill-local-storm-cluster cluster)))))

(defmacro with-local-cluster [[cluster-sym & args] & body]
  `(with-local-cluster*
     (fn [~cluster-sym] ~@body)
     ~@args))

(defmacro with-simulated-time-local-cluster [& args]
  `(with-simulated-time
     (with-local-cluster ~@args)))

;; TODO: should take in a port symbol and find available port automatically

(defn with-inprocess-zookeeper* [f port]
  (with-local-tmp [tmp]
    (let [zks (zk/mk-inprocess-zookeeper tmp port)]
      (try (f)
           (finally (zk/shutdown-inprocess-zookeeper zks))))))

(defmacro with-inprocess-zookeeper [port & body]
  `(with-inprocess-zookeeper*
     (fn [] ~@body)
     ~port))

(defn submit-local-topology [nimbus storm-name conf topology]
  (.submitTopology nimbus storm-name nil (to-json conf) topology))

(defn submit-mocked-assignment [nimbus storm-name conf topology task->component task->node+port]
  (with-var-roots [nimbus/mk-task-component-assignments (fn [& ignored] task->component)
                   nimbus/compute-new-task->node+port (fn [& ignored] task->node+port)]
    (submit-local-topology nimbus storm-name conf topology)
    ))

(defn mk-capture-launch-fn [capture-atom]
  (fn [conf shared-context storm-id supervisor-id port worker-id _]
    (let [existing (get @capture-atom [supervisor-id port] [])]
      (swap! capture-atom assoc [supervisor-id port] (conj existing storm-id))
      )))

(defn find-worker-id [supervisor-conf port]
  (let [supervisor-state (supervisor-state supervisor-conf)
        worker->port (.get supervisor-state common/LS-APPROVED-WORKERS)]
    (first ((reverse-map worker->port) port))
    ))

(defn find-worker-port [supervisor-conf worker-id]
  (let [supervisor-state (supervisor-state supervisor-conf)
        worker->port (.get supervisor-state common/LS-APPROVED-WORKERS)
        ]
    (worker->port worker-id)
    ))

(defn mk-capture-shutdown-fn [capture-atom]
  (let [existing-fn supervisor/shutdown-worker]
    (fn [conf supervisor-id worker-id worker-thread-pids-atom]
      (let [port (find-worker-port conf worker-id)
            existing (get @capture-atom [supervisor-id port] 0)]      
        (swap! capture-atom assoc [supervisor-id port] (inc existing))
        (existing-fn conf supervisor-id worker-id worker-thread-pids-atom)
        ))))

(defmacro capture-changed-workers* [f]
  (let [launch-captured   (atom {})
        shutdown-captured (atom {})]
    (with-var-roots [supervisor/launch-worker (mk-capture-launch-fn launch-captured)
                     supervisor/shutdown-worker (mk-capture-shutdown-fn
                                                 shutdown-captured)]
      (f)
      {:launched @launch-captured
       :shutdown @shutdown-captured})))

(def capture-launched-workers*
  (comp :launched capture-changed-workers*))

(def capture-shutdown-workers*
  (comp :shutdown capture-changed-workers*))

(defmacro capture-changed-workers [& body]
  `(capture-change-workers* (fn [] ~@body)))

(defmacro capture-launched-workers [& body]
  `(capture-launched-workers* (fn [] ~@body)))

(defmacro capture-shutdown-workers [& body]
  `(capture-shutdown-workers* (fn [] ~@body)))

(defnk aggregated-stat [cluster-map storm-name stat-key :component-ids nil]
  (let [state (:storm-cluster-state cluster-map)
        storm-id (common/get-storm-id state storm-name)
        component->tasks (reverse-map
                          (common/storm-task-info
                           state
                           storm-id))
        component->tasks (if component-ids
                           (select-keys component->tasks component-ids)
                           component->tasks)
        task-ids (apply concat (vals component->tasks))
        heartbeats (dofor [id task-ids] (.task-heartbeat state storm-id id))
        stats (dofor [hb heartbeats] (if hb (stat-key (:stats hb)) 0))]
    (reduce + stats)
    ))

(defn emitted-spout-tuples [cluster-map topology storm-name]
  (aggregated-stat cluster-map
                   storm-name
                   :emitted
                   :component-ids (keys (.get_spouts topology))))

(defn transferred-tuples [cluster-map storm-name]
  (aggregated-stat cluster-map storm-name :transferred))

(defn acked-tuples [cluster-map storm-name]
  (aggregated-stat cluster-map storm-name :acked))

(defn simulate-wait [cluster-map]
  (if (Time/isSimulating)
    (advance-cluster-time cluster-map 10)
    (Thread/sleep 100)
    ))


;; TODO: mock-sources needs to be able to mock out state spouts as well
(defnk complete-topology [cluster-map topology :mock-sources {} :storm-conf {}]
  (let [storm-name (str "topologytest-" (uuid))
        state (:storm-cluster-state cluster-map)
        spouts (.get_spouts topology)
        bolts (.get_bolts topology)
        replacements (map-val (fn [v]
                                (FixedTupleSpout.
                                 (for [tup v]
                                   (if (map? tup)
                                     (FixedTuple. (:stream tup) (:values tup))
                                     tup))))
                              mock-sources)
        all-streams (apply concat
                           (for [[id spec] (merge (clojurify-structure spouts) (clojurify-structure bolts))]
                             (for [[stream _] (.. spec get_common get_streams)]
                               (GlobalStreamId. id stream))))
        capturer (TupleCaptureBolt. storm-name)
        ]
    (doseq [[id spout] replacements]
      (let [spout-spec (get spouts id)]
        (.set_spout_object spout-spec (serialize-component-object spout))
        ))
    (doseq [[_ spout-spec] (clojurify-structure spouts)]
      (when-not (instance? FixedTupleSpout (deserialized-component-object (.get_spout_object spout-spec)))
        (throw (RuntimeException. "Cannot complete topology unless every spout is a FixedTupleSpout (or mocked to be)"))
        ))
    
    (.set_bolts topology
                (assoc (clojurify-structure bolts)
                  (uuid)
                  (Bolt.
                   (into {} (for [id all-streams] [id (mk-global-grouping)]))
                   (serialize-component-object capturer)
                   (mk-plain-component-common {} nil))
                  ))
    (submit-local-topology (:nimbus cluster-map) storm-name storm-conf topology)

    
    
    (let [num-source-tuples (reduce +
                                    (for [[_ spout-spec] spouts]
                                      (-> (.get_spout_object spout-spec)
                                          deserialized-component-object
                                          .getSourceTuples
                                          count)
                                      ))
          storm-id (common/get-storm-id state storm-name)]
      (while (< (+ (FixedTupleSpout/getNumAcked storm-id)
                   (FixedTupleSpout/getNumFailed storm-id))
                num-source-tuples)
        (simulate-wait cluster-map))

      (.killTopology (:nimbus cluster-map) storm-name)
      (while (.assignment-info state storm-id nil)
        (simulate-wait cluster-map))
      (FixedTupleSpout/clear storm-id))

    (.getResults capturer)
    ))

(defn read-tuples
  ([results component-id stream-id]
     (let [fixed-tuples (get results component-id [])]
       (mapcat
        (fn [ft]
          (if (= stream-id (. ft stream))
            [(vec (. ft values))]))
        fixed-tuples)
       ))
  ([results component-id]
     (read-tuples results component-id Utils/DEFAULT_STREAM_ID)
     ))

(defn ms= [& args]  
  (apply = (map multi-set args)))

(def TRACKER-BOLT-ID "+++tracker-bolt")

(defn mk-tracked-topology
  "Spouts are of form [spout & options], bolts are of form [inputs bolt & options]"
  [tracked-cluster spouts-map bolts-map]
  (let [track-id (::track-id tracked-cluster)        
        spouts-map (into {}
                         (for [[id [spout & options]] spouts-map]
                           [id
                            (apply mk-spout-spec
                                   (SpoutTracker. spout track-id)
                                   options)]))
        bolts-map (into {}
                        (for [[id [inputs bolt & options]] bolts-map]
                          [id
                           (apply mk-bolt-spec
                                  inputs
                                  (BoltTracker. bolt track-id)
                                  options)]))
        ]
    {:topology (mk-topology spouts-map bolts-map)
     :last-spout-emit (atom 0)
     :cluster tracked-cluster
     }))

(defn assoc-track-id [cluster track-id]
  (assoc cluster ::track-id track-id))

(defn increment-global! [id key]
  (-> (RegisteredGlobalState/getState id)
      (get key)
      .incrementAndGet))

(defn global-amt [id key]
  (-> (RegisteredGlobalState/getState id)
      (get key)
      .get
      ))

(defn with-tracked-cluster* [f & cluster-args]
  (let [id (uuid)]
    (RegisteredGlobalState/setState
     id
     (doto (ConcurrentHashMap.)
       (.put "spout-emitted" (AtomicInteger. 0))
       (.put "transferred" (AtomicInteger. 0))
       (.put "processed" (AtomicInteger. 0))))
    (with-var-roots [acker/mk-acker-bolt
                     (let [old acker/mk-acker-bolt]
                       (fn [& args]
                         (BoltTracker. (apply old args) id)))
                      
                     worker/mk-transfer-fn
                     (let [old worker/mk-transfer-fn]
                       (fn [& args]
                         (let [transferrer (apply old args)]
                           (fn [& transfer-args]
                             ;; (log-message "Transferring: " transfer-args#)
                             (increment-global! id "transferred")
                             (apply transferrer transfer-args)))))]
      (apply with-local-cluster*
             (comp f #(assoc-track-id % id))
             cluster-args))
    (RegisteredGlobalState/clearState id)))

(defmacro with-tracked-cluster [[cluster-sym & cluster-args] & body]
  `(with-tracked-cluster*
     (fn [~cluster-sym] ~@body)
     ~@cluster-args))

(defn tracked-wait
  "Waits until topology is idle and 'amt' more tuples have been emitted by spouts."
  ([tracked-topology]
     (tracked-wait tracked-topology 1))
  ([tracked-topology amt]
      (let [target (+ amt @(:last-spout-emit tracked-topology))
            track-id (-> tracked-topology :cluster ::track-id)
            waiting? (fn []
                       (or (not= target (global-amt track-id "spout-emitted"))
                           (not= (global-amt track-id "transferred")                                 
                                 (global-amt track-id "processed"))
                           ))]
        (while (waiting?)
          (Thread/sleep 5))
        (reset! (:last-spout-emit tracked-topology) target)
        )))
