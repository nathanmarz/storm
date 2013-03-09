(ns backtype.storm.metric.testing
  "This namespace is for AOT dependent metrics testing code."
  (:gen-class))

(letfn [(for- [threader arg seq-exprs body]
          `(reduce #(%2 %1)
                   ~arg
                   (for ~seq-exprs
                     (fn [arg#] (~threader arg# ~@body)))))]
  (defmacro for->
    "Apply a thread expression to a sequence.
   eg.
      (-> 1
        (for-> [x [1 2 3]]
          (+ x)))
   => 7"
    {:indent 1}
    [arg seq-exprs & body]
    (for- 'clojure.core/-> arg seq-exprs body)))

(gen-class
 :name clojure.storm.metric.testing.FakeMetricConsumer
 :implements [backtype.storm.metric.api.IMetricsConsumer]
 :prefix "impl-")

(def buffer (atom nil))

(defn impl-prepare [this conf argument ctx error-reporter]
  (reset! buffer {}))

(defn impl-cleanup [this]
  (reset! buffer {}))

(defn vec-conj [coll x] (if coll
                          (conj coll x)
                          [x]))

(defn expand-complex-datapoint [dp]
  (if (or (map? (.value dp))
          (instance? java.util.AbstractMap (.value dp)))
    (into [] (for [[k v] (.value dp)]
               [(str (.name dp) "/" k) v]))
    [[(.name dp) (.value dp)]]))

(defn impl-handleDataPoints [this task-info data-points]  
  (swap! buffer
         (fn [old]
           (-> old
            (for-> [dp data-points
                    [name val] (expand-complex-datapoint dp)]
                   (update-in [(.srcComponentId task-info) name (.srcTaskId task-info)] vec-conj val))))))
 

