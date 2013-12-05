(def ROOT-DIR (subs *file* 0 (- (count *file*) (count "project.clj"))))
(def VERSION (-> ROOT-DIR (str "/../VERSION") slurp (.trim)))

(eval `(defproject storm/storm-netty ~VERSION
  :dependencies [[storm/storm-core ~VERSION]
                 [io.netty/netty "3.6.3.Final"]]
  :java-source-paths ["src/jvm"]
  :test-paths ["test/clj"]
  :profiles {:release {}}
  :jvm-opts ["-Djava.library.path=/usr/local/lib:/opt/local/lib:/usr/lib"]
  :target-path "target"
  :javac-options ["-target" "1.6" "-source" "1.6"]
  :aot :all))
