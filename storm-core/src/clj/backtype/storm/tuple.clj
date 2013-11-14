(ns backtype.storm.tuple
  (:use [backtype.storm bootstrap])
  )

(bootstrap)

(def ARRAY-TYPES-MAP 
  {
    (type (to-array [])) nil
    (type (byte-array 0)) nil
    (type (char-array 0)) nil
    (type (short-array 0)) nil
    (type (int-array 0)) nil
    (type (long-array 0)) nil
    (type (float-array 0)) nil
    (type (double-array 0)) nil
    (type (boolean-array 0)) nil
  })

(defn deep-hash-code [alist]
  "deep hash code based on array/list content.
   it's the same as java.util.Arrays.deepHashCode(),
   but without convert a list to array to avoid copy."
  (reduce
    #(.intValue (+ (.intValue (* 31 %1)) (hash-code %2)))
    1 alist))

(defn hash-code [obj]
  (let [t (type obj)]
    (if (contains? ARRAY-TYPES-MAP t)
      (deep-hash-code obj)
      (.hashCode obj))))

(defn list-hash-code [^List alist]
  (deep-hash-code alist))

