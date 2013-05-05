(def ROOT-DIR (subs *file* 0 (- (count *file*) (count "project.clj"))))
(def VERSION (-> ROOT-DIR (str "/../VERSION") slurp))

(eval `(defproject storm/storm-netty ~VERSION
  :url "http://storm-project.net"
  :description "Distributed and fault-tolerant realtime computation"
  :license {:name "Eclipse Public License - Version 1.0" :url "https://github.com/nathanmarz/storm/blob/master/LICENSE.html"}
  :mailing-list {:name "Storm user mailing list"
                 :archive "https://groups.google.com/group/storm-user"
                 :post "storm-user@googlegroups.com"}
  :dependencies [[storm/storm-core ~VERSION]
                 [io.netty/netty "3.6.3.Final"]]
  :source-paths ["src/jvm"]
  :java-source-paths ["src/jvm"]
  :test-paths ["test/clj"]
  :profiles {:release {}}
  :aot :all))
