(ns backtype.storm.nimbus.BasicTopologyValidator-test
  (:use [backtype.storm config util])
  (:use [clojure test])
  (:import [backtype.storm.generated StormTopology])
  (:import [backtype.storm.nimbus BasicTopologyValidator])
  (:import [backtype.storm.utils Utils])
  (:import [org.mockito Mockito])
)

(deftest test-valid-non-collection-types
  (let [validator (BasicTopologyValidator.)
        topo (Mockito/mock StormTopology)
        conf (Utils/findAndReadConfigFile "test-valid-non-collection-types.yaml" true)
        ]
    (.validate validator "test-name" conf topo)
  )
)
