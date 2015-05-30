#!/usr/bin/env python

from subprocess import Popen, PIPE
import sys
import os

os.chdir("storm-core")

ns = sys.argv[1]
pipe = Popen(["mvn", "clojure:repl"], stdin=PIPE)


pipe.stdin.write("""
(do
  (use 'clojure.test)
  (require '%s :reload-all)
  (let [results (run-tests '%s)]
    (println results)
    (if (or
         (> (:fail results) 0)
         (> (:error results) 0))
      (System/exit 1)
      (System/exit 0))))
""" % (ns, ns))



pipe.stdin.write("\n")
pipe.stdin.close()
pipe.wait()

os.chdir("..")

sys.exit(pipe.returncode)
