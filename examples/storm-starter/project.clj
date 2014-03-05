(defproject storm-starter "0.0.1-SNAPSHOT"
  :source-paths ["src/clj"]
  :java-source-paths ["src/jvm"]
  :test-paths ["test/jvm"]
  :resource-paths ["multilang"]
  :aot :all
  :repositories {
;;                 "twitter4j" "http://twitter4j.org/maven2"
                 }

  :dependencies [
;;                 [org.twitter4j/twitter4j-core "2.2.6-SNAPSHOT"]
;;                 [org.twitter4j/twitter4j-stream "2.2.6-SNAPSHOT"]
                   [commons-collections/commons-collections "3.2.1"]
                 ]

  :profiles {:dev
              {:dependencies [[storm "0.9.0.1"]
                              [org.clojure/clojure "1.4.0"]
                              [org.testng/testng "6.8.5"]
                              [org.easytesting/fest-assert-core "2.0M8"]
                              [org.mockito/mockito-all "1.9.0"]
                              [org.jmock/jmock "2.6.0"]]}}
  :min-lein-version "2.0.0"
  )
