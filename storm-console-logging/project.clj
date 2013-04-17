(def ROOT-DIR (subs *file* 0 (- (count *file*) (count "project.clj"))))
(def VERSION (-> ROOT-DIR (str "/../VERSION") slurp))

(defproject storm/storm-console-logging VERSION
  :resource-paths ["logback"]

  :profiles {:release {}
             }

  :aot :all)
