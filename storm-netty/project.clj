(def ROOT-DIR (subs *file* 0 (- (count *file*) (count "project.clj"))))
(def VERSION (-> ROOT-DIR (str "/../VERSION") slurp))

(defproject storm/storm-netty VERSION
  :dependencies [[storm/storm-core "0.9.0-wip17"]
                 [io.netty/netty "3.6.3.Final"]]
  
  :source-paths ["src/jvm"]
  :java-source-paths ["src/jvm"]
  :test-paths ["test/clj"]
  
  :profiles {:release {}}
  :aot :all)