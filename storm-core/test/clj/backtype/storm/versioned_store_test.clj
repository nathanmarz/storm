(ns backtype.storm.versioned-store-test
  (:use [clojure test])
  (:use [backtype.storm testing])
  (:import [backtype.storm.utils VersionedStore]))

(defmacro defvstest [name [vs-sym] & body]
  `(deftest ~name
    (with-local-tmp [dir#]
      (let [~vs-sym (VersionedStore. dir#)]
        ~@body
        ))))

(defvstest test-empty-version [vs]
  (let [v (.createVersion vs)]
    (.succeedVersion vs v)
    (is (= 1 (count (.getAllVersions vs))))
    (is (= v (.mostRecentVersionPath vs)))
    ))

(defvstest test-multiple-versions [vs]
  (.succeedVersion vs (.createVersion vs))
  (Thread/sleep 100)
  (let [v (.createVersion vs)]
    (.succeedVersion vs v)
    (is (= 2 (count (.getAllVersions vs))))
    (is (= v (.mostRecentVersionPath vs)))
    
    (.createVersion vs)
    (is (= v (.mostRecentVersionPath vs)))
    ))
