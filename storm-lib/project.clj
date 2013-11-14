(def ROOT-DIR (subs *file* 0 (- (count *file*) (count "project.clj"))))
(def VERSION (-> ROOT-DIR (str "/../VERSION") slurp (.trim)))
(def MODULES (-> ROOT-DIR (str "/../MODULES") slurp (.split "\n") (#(filter (fn [m] (not= m "storm-console-logging")) %)) ))
(def DEPENDENCIES (for [m MODULES] [(symbol (str "storm/" m)) VERSION]))

;; for lib pom.xml, change the symbol to storm/storm-liba and filter out storm-console-logging from modules

(eval `(defproject storm/storm-lib ~VERSION
  :url "http://storm-project.net"
  :description "Distributed and fault-tolerant realtime computation"
  :license {:name "Eclipse Public License - Version 1.0" :url "https://github.com/nathanmarz/storm/blob/master/LICENSE.html"}
  :mailing-list {:name "Storm user mailing list"
                 :archive "https://groups.google.com/group/storm-user"
                 :post "storm-user@googlegroups.com"}
  :dependencies [~@DEPENDENCIES]
  :min-lein-version "2.0.0"
  :target-path "target"
  ))
