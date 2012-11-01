#!/bin/bash
touch /tmp/storm-classloader-test.txt
lein jar
../../bin/storm run backtype.storm.testing.TestClassLoaderTopology *.jar
cat /tmp/storm-classloader-test.txt
# remove the tmp file
rm /tmp/storm-classloader-test.txt