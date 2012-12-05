(defproject storm/storm-kafka "0.9.0-wip5-scala292-multischeme"
  :source-path "src/clj"
  :java-source-path "src/jvm"
  :javac-options {:debug "true" :fork "true"}
  :repositories {;;"releases" "http://artifactory.local.twitter.com/libs-releases-local"
                 ;;"snapshots" "http://artifactory.local.twitter.com/libs-snapshots-local"
                 "conjars" "http://conjars.org/repo/"}
  :dependencies [[com.twitter/kafka_2.9.2 "0.7.0"
                  :exclusions [org.apache.zookeeper/zookeeper
                               log4j/log4j]]]
  :dev-dependencies [[storm "0.9.0-wip9"]
                     [org.slf4j/log4j-over-slf4j "1.6.6"]
                     [ch.qos.logback/logback-classic "1.0.6"]
                     [org.clojure/clojure "1.4.0"]]
  :jvm-opts ["-Djava.library.path=/usr/local/lib:/opt/local/lib:/usr/lib"])
