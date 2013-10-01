(def VERSION (.trim (slurp "VERSION")))
(def MODULES (-> "MODULES" slurp (.split "\n")))
(def DEPENDENCIES (for [m MODULES] [(symbol (str "storm/" m)) VERSION]))

(eval `(defproject storm/storm ~VERSION
  :url "http://storm-project.net"
  :description "Distributed and fault-tolerant realtime computation"
  :license {:name "Eclipse Public License - Version 1.0" :url "https://github.com/nathanmarz/storm/blob/master/LICENSE.html"}
  :mailing-list {:name "Storm user mailing list"
                 :archive "https://groups.google.com/group/storm-user"
                 :post "storm-user@googlegroups.com"}
  :dependencies [~@DEPENDENCIES]
  :plugins [[~'lein-sub "0.2.1"],[no-man-is-an-island/lein-eclipse "2.0.0"]]
  :min-lein-version "2.0.0"
  :sub [~@MODULES]
  ))
