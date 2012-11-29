(defproject storm "0.8.2-wip15"
  :description "Storm is a distributed realtime computation system."
  :url "http://www.storm-project.net"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :mailing-list {:name "Storm user mailing list"
                 :archive "https://groups.google.com/d/forum/storm-user"
                 :post "storm-user@googlegroups.com"}
  :min-lein-version "2.0.0"
  :jvm-opts ["-Djava.library.path=/usr/local/lib:/opt/local/lib:/usr/lib"]
  :source-paths ["src/clj" "src/ui"]
  :test-paths ["test/clj"]
  :java-source-paths ["src/jvm"]
  :javac-options {:debug "true"}
  :resources-paths ["conf"]
  :repositories {"sonatype" "http://oss.sonatype.org/content/groups/public/"}
  :dependencies [[org.clojure/clojure "1.4.0"]
                 [commons-io "1.4"]
                 [org.apache.commons/commons-exec "1.1"]
                 [storm/libthrift7 "0.7.0"]
                 [clj-time "0.4.1"]
                 [log4j/log4j "1.2.16"]
                 [com.netflix.curator/curator-framework "1.0.1"]
                 [backtype/jzmq "2.1.0"]
                 [com.googlecode.json-simple/json-simple "1.1"]
                 [compojure "0.6.4"]
                 [hiccup "0.3.6"]
                 [ring/ring-jetty-adapter "0.3.11"]
                 [org.clojure/tools.logging "0.2.3"]
                 [org.clojure/math.numeric-tower "0.0.1"]
                 [org.slf4j/slf4j-log4j12 "1.5.8"]
                 [storm/carbonite "1.5.0"]
                 [org.yaml/snakeyaml "1.9"]
                 [org.apache.httpcomponents/httpclient "4.1.1"]
                 [storm/tools.cli "0.2.2"]
                 [com.googlecode.disruptor/disruptor "2.10.1"]
                 [storm/jgrapht "0.8.3"]
                 [com.google.guava/guava "13.0"]]
  :profiles {:dev {:resource-paths ["src/dev"]
                   :dependencies
                   [[swank-clojure "1.4.0-SNAPSHOT" :exclusions [org.clojure/clojure]]]}}
  :aot :all)
