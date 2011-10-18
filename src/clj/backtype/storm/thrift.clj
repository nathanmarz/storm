(ns backtype.storm.thrift
  (:import [backtype.storm.generated Grouping Nimbus StormTopology Bolt Nimbus$Client Nimbus$Iface ComponentCommon Grouping$_Fields SpoutSpec NullStruct StreamInfo GlobalStreamId ComponentObject ComponentObject$_Fields ShellComponent])
  (:import [backtype.storm.utils Utils])
  (:import [backtype.storm Constants])
  (:import [backtype.storm.task CoordinatedBolt CoordinatedBolt$SourceArgs])
  (:import [backtype.storm.topology OutputFieldsGetter IBasicBolt BasicBoltExecutor])
  (:import [org.apache.thrift.protocol TBinaryProtocol TProtocol])
  (:import [org.apache.thrift.transport TTransport TFramedTransport TSocket])
  (:use [backtype.storm util])
  )

(def grouping-constants
  {Grouping$_Fields/FIELDS :fields
   Grouping$_Fields/SHUFFLE :shuffle
   Grouping$_Fields/ALL :all
   Grouping$_Fields/NONE :none
   Grouping$_Fields/DIRECT :direct
  })

(defn grouping-type [^Grouping grouping]
  (grouping-constants (.getSetField grouping)))

(defn field-grouping [^Grouping grouping]
  (when-not (= (grouping-type grouping) :fields)
    (throw (IllegalArgumentException. "Tried to get grouping fields from non fields grouping")))
  (.get_fields grouping))

(defn global-grouping? [^Grouping grouping]
  (and (= :fields (grouping-type grouping))
       (empty? (field-grouping grouping))
       ))

(defn parallelism-hint [^ComponentCommon component-common]
  (let [phint (.get_parallelism_hint component-common)]
    (if (= phint 0) 1 phint)
    ))

(defn nimbus-client-and-conn [host port]
  (let [transport (TFramedTransport. (TSocket. host port))
        prot (TBinaryProtocol. transport)
        client (Nimbus$Client. prot)]
        (.open transport)
        [client transport] ))

(defmacro with-nimbus-connection [[client-sym host port] & body]
  `(let [[^Nimbus$Client ~client-sym ^TTransport conn#] (nimbus-client-and-conn ~host ~port)]
      (try
        ~@body
      (finally (.close conn#)))
      ))

(defn mk-component-common [component parallelism-hint]
  (let [getter (OutputFieldsGetter.)
        _ (.declareOutputFields component getter)
        ret (ComponentCommon. (.getFieldsDeclaration getter))]
    (when parallelism-hint
      (.set_parallelism_hint ret parallelism-hint))
    ret
    ))

(defn direct-output-fields [fields]
  (StreamInfo. fields true))

(defn mk-output-spec [output-spec]
  (let [output-spec (if (map? output-spec) output-spec {Utils/DEFAULT_STREAM_ID output-spec})]
    (map-val
      (fn [out]
        (if (instance? StreamInfo out)
          out
          (StreamInfo. out false)
          ))
      output-spec
      )))

(defn mk-plain-component-common [output-spec parallelism-hint]
  (let [ret (ComponentCommon. (mk-output-spec output-spec))]
    (when parallelism-hint
      (.set_parallelism_hint ret parallelism-hint))
    ret
    ))

(defnk mk-spout-spec [spout :parallelism-hint nil :p nil]
  ;; for backwards compatibility
  (let [parallelism-hint (if p p parallelism-hint)]
    (SpoutSpec. (ComponentObject/serialized_java (Utils/serialize spout))
                (mk-component-common spout parallelism-hint)
                (.isDistributed spout))
    ))

(defn mk-shuffle-grouping []
  (Grouping/shuffle (NullStruct.)))

(defn mk-fields-grouping [fields]
  (Grouping/fields fields))

(defn mk-global-grouping []
  (mk-fields-grouping []))

(defn mk-direct-grouping []
  (Grouping/direct (NullStruct.)))

(defn mk-all-grouping []
  (Grouping/all (NullStruct.)))

(defn mk-none-grouping []
  (Grouping/none (NullStruct.)))

(defn deserialized-component-object [^ComponentObject obj]
  (when (not= (.getSetField obj) ComponentObject$_Fields/SERIALIZED_JAVA)
    (throw (RuntimeException. "Cannot deserialize non-java-serialized object")))
  (Utils/deserialize (.get_serialized_java obj))
  )

(defn serialize-component-object [obj]
  (ComponentObject/serialized_java (Utils/serialize obj)))

(defn mk-grouping [grouping-spec]
  (cond (nil? grouping-spec) (mk-none-grouping)
        (instance? Grouping grouping-spec) grouping-spec
        (sequential? grouping-spec) (mk-fields-grouping grouping-spec)
        (= grouping-spec :shuffle) (mk-shuffle-grouping)
        (= grouping-spec :none) (mk-none-grouping)
        (= grouping-spec :all) (mk-all-grouping)
        (= grouping-spec :global) (mk-global-grouping)
        (= grouping-spec :direct) (mk-direct-grouping)
        true (throw (IllegalArgumentException. (str grouping-spec " is not a valid grouping")))
        ))

(defn- mk-inputs [inputs]
  (into {}
    (for [[stream-id grouping-spec] inputs]
      [(if (sequential? stream-id)
         (GlobalStreamId. (first stream-id) (second stream-id))
         (GlobalStreamId. stream-id (Utils/DEFAULT_STREAM_ID)))
       (mk-grouping grouping-spec)]
      )))

(defnk mk-bolt-spec [inputs bolt :parallelism-hint nil :p nil]
  ;; for backwards compatibility
  (let [parallelism-hint (if p p parallelism-hint)
        bolt (if (instance? IBasicBolt bolt) (BasicBoltExecutor. bolt) bolt)]
    (Bolt.
     (mk-inputs inputs)
     (ComponentObject/serialized_java (Utils/serialize bolt))
     (mk-component-common bolt parallelism-hint)
     )))

(defnk mk-shell-bolt-spec [inputs command script output-spec :parallelism-hint nil :p nil]
  ;; for backwards compatibility
  (let [parallelism-hint (if p p parallelism-hint)]
    (Bolt.
     (mk-inputs inputs)
     (ComponentObject/shell (ShellComponent. command script))
     (mk-plain-component-common output-spec parallelism-hint)
     )))

(defn mk-topology
  ([spout-map bolt-map]
     (StormTopology. spout-map bolt-map {}))
  ([spout-map bolt-map state-spout-map]
     (StormTopology. spout-map bolt-map state-spout-map)))

(defnk coordinated-bolt [bolt :type nil :all-out false]
  (let [source (condp = type
                   nil nil
                   :all (CoordinatedBolt$SourceArgs/all)
                   :single (CoordinatedBolt$SourceArgs/single))]
    (CoordinatedBolt. bolt source all-out)
    ))

(def COORD-STREAM Constants/COORDINATED_STREAM_ID)


