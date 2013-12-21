;; Licensed to the Apache Software Foundation (ASF) under one
;; or more contributor license agreements.  See the NOTICE file
;; distributed with this work for additional information
;; regarding copyright ownership.  The ASF licenses this file
;; to you under the Apache License, Version 2.0 (the
;; "License"); you may not use this file except in compliance
;; with the License.  You may obtain a copy of the License at
;;
;; http:;; www.apache.org/licenses/LICENSE-2.0
;;
;; Unless required by applicable law or agreed to in writing, software
;; distributed under the License is distributed on an "AS IS" BASIS,
;; WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
;; See the License for the specific language governing permissions and
;; limitations under the License.
(def ROOT-DIR (subs *file* 0 (- (count *file*) (count "project.clj"))))
(def VERSION (-> ROOT-DIR (str "/../VERSION") slurp (.trim)))

(defproject org.apache.storm/storm-core VERSION
  :dependencies [[org.clojure/clojure "1.4.0"]
                 [commons-io "1.4"]
                 [org.apache.commons/commons-exec "1.1"]
                 [storm/libthrift7 "0.7.0-2"
                  :exclusions [org.slf4j/slf4j-api]]
                 [clj-time "0.4.1"]
                 [com.netflix.curator/curator-framework "1.0.1"
                  :exclusions [log4j/log4j]]
                 [com.googlecode.json-simple/json-simple "1.1"]
                 [compojure "1.1.3"]
                 [hiccup "0.3.6"]
                 [ring/ring-devel "0.3.11"]
                 [ring/ring-jetty-adapter "0.3.11"]
                 [org.clojure/tools.logging "0.2.3"]
                 [org.clojure/math.numeric-tower "0.0.1"]
                 [com.twitter/carbonite "1.3.2"]
                 [org.yaml/snakeyaml "1.11"]
                 [org.apache.httpcomponents/httpclient "4.1.1"]
                 [org.clojure/tools.cli "0.2.2"]
                 [com.googlecode.disruptor/disruptor "2.10.1"]
                 [org.jgrapht/jgrapht-core "0.9.0"]
                 [com.google.guava/guava "13.0"]
                 [ch.qos.logback/logback-classic "1.0.6"]
                 [org.slf4j/log4j-over-slf4j "1.6.6"]
                 [io.netty/netty "3.6.3.Final"]
                 ]

  :source-paths ["src/clj"]
  :java-source-paths ["src/jvm"]
  :test-paths ["test/clj"]
  :resource-paths ["../conf"]
  :target-path "target"
  :javac-options ["-target" "1.6" "-source" "1.6" "-g"]
  :profiles {:dev {:resource-paths ["src/dev"]
                   :dependencies [[org.mockito/mockito-all "1.9.5"]]}
             :release {}
             :lib {}
             }

  :plugins [[lein-swank "1.4.4"]]

  :repositories {"sonatype"
                 "http://oss.sonatype.org/content/groups/public/"}

  :jvm-opts ["-Djava.library.path=/usr/local/lib:/opt/local/lib:/usr/lib"]

  :aot :all)
