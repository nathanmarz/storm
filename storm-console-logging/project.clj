(def ROOT-DIR (subs *file* 0 (- (count *file*) (count "project.clj"))))
(def VERSION (-> ROOT-DIR (str "/../VERSION") slurp (.trim)))

(defproject storm/storm-console-logging VERSION
  :resource-paths ["logback"]
  :target-path "target"
  :javac-options ["-target" "1.6" "-source" "1.6"]
  :profiles {:release {}
             }

  :aot :all)
