(def ROOT-DIR (subs *file* 0 (- (count *file*) (count "project.clj"))))
(def VERSION (-> ROOT-DIR (str "/../VERSION") slurp (.trim)))

(defproject storm/storm-console-logging VERSION
  :dependencies [[org.clojure/clojure "1.4.0"]
                ]
  :resource-paths ["logback"]

  :profiles {
             :release { :target-path "target" }
             }
  :aot :all)
