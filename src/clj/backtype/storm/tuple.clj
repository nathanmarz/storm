(ns backtype.storm.tuple
  (:use [backtype.storm bootstrap])
  )

(bootstrap)

(defn list-hash-code [^List alist]
  (.hashCode alist))
