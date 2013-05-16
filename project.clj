(defproject storm/storm-kafka-0.8-plus "0.1.0-SNAPSHOT"
  :description "Storm module for kafka > 0.8"
  :plugins [[lein-junit "1.1.2"]]
  :java-source-paths ["src/jvm"]
  :test-paths ["src/test"]
  :junit ["src/test"]

  :repositories {"scala-tools" "http://scala-tools.org/repo-releases"
                 "conjars" "http://conjars.org/repo/"}
  :dependencies [[org.scala-lang/scala-library "2.9.2"]
                 [junit/junit "4.11" :scope "test"]
                 [com.netflix.curator/curator-framework "1.3.2"
                  :exclusions [log4j/log4j
                               org.slf4j/slf4j-log4j12] ]
                 [com.netflix.curator/curator-recipes "1.2.6"
                  :exclusions [log4j/log4j] :scope "test"]
                 [com.netflix.curator/curator-test "1.2.6"
                  :exclusions [log4j/log4j] :scope "test"]
                 [org.apache/kafka_2.9.2 "0.8.0-SNAPSHOT"
                  :exclusions [org.apache.zookeeper/zookeeper
                               log4j/log4j]]]
  :profiles {:provided {:dependencies [[storm "0.9.0-wip15"]
                                       [org.slf4j/log4j-over-slf4j "1.6.6"]
                                       ;;[ch.qos.logback/logback-classic "1.0.6"]
                                       [org.clojure/clojure "1.4.0"]]}}
  :jvm-opts ["-Djava.library.path=/usr/local/lib:/opt/local/lib:/usr/lib"]
  :min-lein-version "2.0")
