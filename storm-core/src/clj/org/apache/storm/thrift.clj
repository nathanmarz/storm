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

(ns org.apache.storm.thrift
  (:import [java.util HashMap]
           [java.io Serializable]
           [org.apache.storm.generated NodeInfo Assignment])
  (:import [org.apache.storm.generated JavaObject Grouping Nimbus StormTopology
            StormTopology$_Fields Bolt Nimbus$Client Nimbus$Iface
            ComponentCommon Grouping$_Fields SpoutSpec NullStruct StreamInfo
            GlobalStreamId ComponentObject ComponentObject$_Fields
            ShellComponent SupervisorInfo])
  (:import [org.apache.storm.utils Utils NimbusClient])
  (:import [org.apache.storm Constants])
  (:import [org.apache.storm.security.auth ReqContext])
  (:import [org.apache.storm.grouping CustomStreamGrouping])
  (:import [org.apache.storm.topology TopologyBuilder])
  (:import [org.apache.storm.clojure RichShellBolt RichShellSpout])
  (:import [org.apache.thrift.transport TTransport])
  (:use [org.apache.storm util config log zookeeper]))

(defn instantiate-java-object
  [^JavaObject obj]
  (let [name (symbol (.get_full_class_name obj))
        args (map (memfn getFieldValue) (.get_args_list obj))]
    (eval `(new ~name ~@args))))

(def grouping-constants
  {Grouping$_Fields/FIELDS :fields
   Grouping$_Fields/SHUFFLE :shuffle
   Grouping$_Fields/ALL :all
   Grouping$_Fields/NONE :none
   Grouping$_Fields/CUSTOM_SERIALIZED :custom-serialized
   Grouping$_Fields/CUSTOM_OBJECT :custom-object
   Grouping$_Fields/DIRECT :direct
   Grouping$_Fields/LOCAL_OR_SHUFFLE :local-or-shuffle})

(defn grouping-type
  [^Grouping grouping]
  (grouping-constants (.getSetField grouping)))

(defn field-grouping
  [^Grouping grouping]
  (when-not (= (grouping-type grouping) :fields)
    (throw (IllegalArgumentException. "Tried to get grouping fields from non fields grouping")))
  (.get_fields grouping))

(defn global-grouping?
  [^Grouping grouping]
  (and (= :fields (grouping-type grouping))
       (empty? (field-grouping grouping))))

(defn parallelism-hint
  [^ComponentCommon component-common]
  (let [phint (.get_parallelism_hint component-common)]
    (if-not (.is_set_parallelism_hint component-common) 1 phint)))

(defn nimbus-client-and-conn
  ([host port]
    (nimbus-client-and-conn host port nil))
  ([host port as-user]
  (log-message "Connecting to Nimbus at " host ":" port " as user: " as-user)
  (let [conf (read-storm-config)
        nimbusClient (NimbusClient. conf host port nil as-user)
        client (.getClient nimbusClient)
        transport (.transport nimbusClient)]
        [client transport] )))

(defmacro with-nimbus-connection
  [[client-sym host port] & body]
  `(let [[^Nimbus$Client ~client-sym ^TTransport conn#] (nimbus-client-and-conn ~host ~port)]
    (try
      ~@body
    (finally (.close conn#)))))

(defmacro with-configured-nimbus-connection
  [client-sym & body]
  `(let [conf# (read-storm-config)
         context# (ReqContext/context)
         user# (if (.principal context#) (.getName (.principal context#)))
         nimbusClient# (NimbusClient/getConfiguredClientAs conf# user#)
         ~client-sym (.getClient nimbusClient#)
         conn# (.transport nimbusClient#)
         ]
     (try
       ~@body
     (finally (.close conn#)))))

(defn direct-output-fields
  [fields]
  (StreamInfo. fields true))

(defn output-fields
  [fields]
  (StreamInfo. fields false))

(defn mk-output-spec
  [output-spec]
  (let [output-spec (if (map? output-spec)
                      output-spec
                      {Utils/DEFAULT_STREAM_ID output-spec})]
    (map-val
      (fn [out]
        (if (instance? StreamInfo out)
          out
          (StreamInfo. out false)))
      output-spec)))

(defnk mk-plain-component-common
  [inputs output-spec parallelism-hint :conf nil]
  (let [ret (ComponentCommon. (HashMap. inputs) (HashMap. (mk-output-spec output-spec)))]
    (when parallelism-hint
      (.set_parallelism_hint ret parallelism-hint))
    (when conf
      (.set_json_conf ret (to-json conf)))
    ret))

(defnk mk-spout-spec*
  [spout outputs :p nil :conf nil]
  (SpoutSpec. (ComponentObject/serialized_java (Utils/javaSerialize spout))
              (mk-plain-component-common {} outputs p :conf conf)))

(defn mk-shuffle-grouping
  []
  (Grouping/shuffle (NullStruct.)))

(defn mk-local-or-shuffle-grouping
  []
  (Grouping/local_or_shuffle (NullStruct.)))

(defn mk-fields-grouping
  [fields]
  (Grouping/fields fields))

(defn mk-global-grouping
  []
  (mk-fields-grouping []))

(defn mk-direct-grouping
  []
  (Grouping/direct (NullStruct.)))

(defn mk-all-grouping
  []
  (Grouping/all (NullStruct.)))

(defn mk-none-grouping
  []
  (Grouping/none (NullStruct.)))

(defn deserialized-component-object
  [^ComponentObject obj]
  (when (not= (.getSetField obj) ComponentObject$_Fields/SERIALIZED_JAVA)
    (throw (RuntimeException. "Cannot deserialize non-java-serialized object")))
  (Utils/javaDeserialize (.get_serialized_java obj) Serializable))

(defn serialize-component-object
  [obj]
  (ComponentObject/serialized_java (Utils/javaSerialize obj)))

(defn- mk-grouping
  [grouping-spec]
  (cond (nil? grouping-spec)
        (mk-none-grouping)

        (instance? Grouping grouping-spec)
        grouping-spec

        (instance? CustomStreamGrouping grouping-spec)
        (Grouping/custom_serialized (Utils/javaSerialize grouping-spec))

        (instance? JavaObject grouping-spec)
        (Grouping/custom_object grouping-spec)

        (sequential? grouping-spec)
        (mk-fields-grouping grouping-spec)

        (= grouping-spec :shuffle)
        (mk-shuffle-grouping)

        (= grouping-spec :local-or-shuffle)
        (mk-local-or-shuffle-grouping)
        (= grouping-spec :none)
        (mk-none-grouping)

        (= grouping-spec :all)
        (mk-all-grouping)

        (= grouping-spec :global)
        (mk-global-grouping)

        (= grouping-spec :direct)
        (mk-direct-grouping)

        true
        (throw (IllegalArgumentException.
                 (str grouping-spec " is not a valid grouping")))))

(defn- mk-inputs
  [inputs]
  (into {} (for [[stream-id grouping-spec] inputs]
             [(if (sequential? stream-id)
                (GlobalStreamId. (first stream-id) (second stream-id))
                (GlobalStreamId. stream-id Utils/DEFAULT_STREAM_ID))
              (mk-grouping grouping-spec)])))

(defnk mk-bolt-spec*
  [inputs bolt outputs :p nil :conf nil]
  (let [common (mk-plain-component-common (mk-inputs inputs) outputs p :conf conf)]
    (Bolt. (ComponentObject/serialized_java (Utils/javaSerialize bolt))
           common)))

(defnk mk-spout-spec
  [spout :parallelism-hint nil :p nil :conf nil]
  (let [parallelism-hint (if p p parallelism-hint)]
    {:obj spout :p parallelism-hint :conf conf}))

(defn- shell-component-params
  [command script-or-output-spec kwargs]
  (if (string? script-or-output-spec)
    [(into-array String [command script-or-output-spec])
     (first kwargs)
     (rest kwargs)]
    [(into-array String command)
     script-or-output-spec
     kwargs]))

(defnk mk-bolt-spec
  [inputs bolt :parallelism-hint nil :p nil :conf nil]
  (let [parallelism-hint (if p p parallelism-hint)]
    {:obj bolt :inputs inputs :p parallelism-hint :conf conf}))

(defn mk-shell-bolt-spec
  [inputs command script-or-output-spec & kwargs]
  (let [[command output-spec kwargs]
        (shell-component-params command script-or-output-spec kwargs)]
    (apply mk-bolt-spec inputs
           (RichShellBolt. command (mk-output-spec output-spec)) kwargs)))

(defn mk-shell-spout-spec
  [command script-or-output-spec & kwargs]
  (let [[command output-spec kwargs]
        (shell-component-params command script-or-output-spec kwargs)]
    (apply mk-spout-spec
           (RichShellSpout. command (mk-output-spec output-spec)) kwargs)))

(defn- add-inputs
  [declarer inputs]
  (doseq [[id grouping] (mk-inputs inputs)]
    (.grouping declarer id grouping)))

(defn mk-topology
  ([spout-map bolt-map]
   (let [builder (TopologyBuilder.)]
     (doseq [[name {spout :obj p :p conf :conf}] spout-map]
       (-> builder (.setSpout name spout (if-not (nil? p) (int p) p)) (.addConfigurations conf)))
     (doseq [[name {bolt :obj p :p conf :conf inputs :inputs}] bolt-map]
       (-> builder (.setBolt name bolt (if-not (nil? p) (int p) p)) (.addConfigurations conf) (add-inputs inputs)))
     (.createTopology builder)))
  ([spout-map bolt-map state-spout-map]
   (mk-topology spout-map bolt-map)))

;; clojurify-structure is needed or else every element becomes the same after successive calls
;; don't know why this happens
(def STORM-TOPOLOGY-FIELDS
  (-> StormTopology/metaDataMap clojurify-structure keys))

(def SPOUT-FIELDS
  [StormTopology$_Fields/SPOUTS
   StormTopology$_Fields/STATE_SPOUTS])

