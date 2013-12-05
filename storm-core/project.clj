(def ROOT-DIR (subs *file* 0 (- (count *file*) (count "project.clj"))))
(def VERSION (-> ROOT-DIR (str "/../VERSION") slurp (.trim)))

(defproject storm/storm-core VERSION
  :dependencies [[org.clojure/clojure "1.4.0"]
                 [commons-io "1.4"]
                 [org.apache.commons/commons-exec "1.1"]
                 [storm/libthrift7 "0.7.0-2"
                  :exclusions [org.slf4j/slf4j-api]]
                 [clj-time "0.4.1"]
                 [com.netflix.curator/curator-framework "1.0.1"
                  :exclusions [log4j/log4j]]
                 [backtype/jzmq "2.1.0"]
                 [com.googlecode.json-simple/json-simple "1.1"]
                 [compojure "1.1.3"]
                 [hiccup "0.3.6"]
                 [ring/ring-devel "0.3.11"]
                 [ring/ring-jetty-adapter "0.3.11"]
                 [org.clojure/tools.logging "0.2.3"]
                 [org.clojure/math.numeric-tower "0.0.1"]
                 [storm/carbonite "1.5.0"]
                 [org.yaml/snakeyaml "1.11"]
                 [org.apache.httpcomponents/httpclient "4.1.1"]
                 [storm/tools.cli "0.2.2"]
                 [com.googlecode.disruptor/disruptor "2.10.1"]
                 [storm/jgrapht "0.8.3"]
                 [com.google.guava/guava "13.0"]
                 [ch.qos.logback/logback-classic "1.0.6"]
                 [org.slf4j/log4j-over-slf4j "1.6.6"]
                 ]

  :source-paths ["src/clj"]
  :java-source-paths ["src/jvm"]
  :test-paths ["test/clj"]
  :resource-paths ["../conf"]
  :target-path "target"
  :javac-options ["-target" "1.6" "-source" "1.6"]
  :profiles {:dev {:resource-paths ["src/dev"]
                   :dependencies [[org.mockito/mockito-all "1.9.5"]]}
             :release {}
             :lib {}
             }

  :plugins [[lein-swank "1.4.4"]]

  :repositories {"sonatype"
                 "http://oss.sonatype.org/content/groups/public/"}

  :javac-options ["-g"]
  :jvm-opts ["-Djava.library.path=/usr/local/lib:/opt/local/lib:/usr/lib"]

  :aot :all)
