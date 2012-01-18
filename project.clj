(defproject storm/storm-kafka "0.7.0-SNAPSHOT"
  :source-path "src/clj"
  :java-source-path "src/jvm"
  :javac-options {:debug "true" :fork "true"}
  :dependencies [[storm/kafka "0.6.0"
                   :exclusions [org.apache.zookeeper/zookeeper]]]
  :dev-dependencies [[storm "0.7.0-SNAPSHOT"]]
  :jvm-opts ["-Djava.library.path=/usr/local/lib:/opt/local/lib:/usr/lib"]
)
