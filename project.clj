(defproject storm "0.8.0-SNAPSHOT"
  :source-path "src/clj"
  :test-path "test/clj"
  :java-source-path "src/jvm"
  :javac-options {:debug "true" :fork "true"}
  :resources-path "conf"
  :dev-resources-path "src/dev"
  :repositories {"sonatype" "http://oss.sonatype.org/content/groups/public/"}
  :dependencies [[org.clojure/clojure "1.4.0"]
                 [commons-io "1.4"]
                 [org.apache.commons/commons-exec "1.1"]
                 [clj-time "0.4.1"]
                 [log4j/log4j "1.2.16"]
                 [com.netflix.curator/curator-framework "1.1.3"]
                 [backtype/jzmq "2.1.0"]
                 [com.googlecode.json-simple/json-simple "1.1"]
                 [com.googlecode/kryo "1.04"]
                 [compojure "0.6.4"]
                 [hiccup "0.3.6"]
                 [ring/ring-jetty-adapter "0.3.11"]
                 [org.clojure/tools.logging "0.2.3"]
                 [org.clojure/math.numeric-tower "0.0.1"]
                 [org.slf4j/slf4j-log4j12 "1.6.4"]
                 [storm/carbonite "1.0.1"]
                 [org.yaml/snakeyaml "1.9"]
                 [org.apache.httpcomponents/httpclient "4.1.1"]
                 [storm/libthrift7 "0.7.0"]
                 [storm/tools.cli "0.2.2"]
                 ]
  :dev-dependencies [
                     [swank-clojure "1.4.0-SNAPSHOT" :exclusions [org.clojure/clojure]]
                    ]
  :jvm-opts ["-Djava.library.path=/usr/local/lib:/opt/local/lib:/usr/lib"]
  :extra-classpath-dirs ["src/ui"]
  :aot :all
)
