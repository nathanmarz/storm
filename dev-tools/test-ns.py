#!/usr/bin/env python

from subprocess import Popen, PIPE
import sys
import os

os.chdir("storm-core")

ns = sys.argv[1]
pipe = Popen(["mvn", "clojure:repl"], stdin=PIPE)

pipe.stdin.write("(do (use 'clojure.test) (require '%s :reload-all) (run-tests '%s))\n" % (ns, ns))
pipe.stdin.write("\n")
pipe.stdin.close()
pipe.wait()

os.chdir("..")
