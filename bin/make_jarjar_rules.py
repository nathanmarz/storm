# rule java.** @0
# rule javax.** @0
# rule backtype.** @0
# rule zilch.** @0
# rule src.jvm.** @0
# rule clojure.** @0
# rule *.** backtype.storm.dep.@0

import sys

alltoks = list()

for line in sys.stdin.readlines():
  tokens = tuple(line.split("/")[0:-1][0:2])
  if(len(tokens) > 0):
    alltoks.append(tokens)

alltoks = sorted(alltoks, key=len)

tochange = set()

for toks in alltoks:
  if(not tuple([toks[0]]) in tochange):
    tochange.add(toks)

for toks in set(tochange):
  if(toks[0] in set(["clojure", "javax", "backtype", "zilch", "java"])):
    tochange.remove(toks)

for toks in tochange:
  print "rule " + ".".join(toks) + ".** backtype.storm.dep.@0"