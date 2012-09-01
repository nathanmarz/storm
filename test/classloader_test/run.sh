lein jar
../../bin/storm run backtype.storm.testing.TestClassLoaderTopology *.jar
cat /tmp/storm-classloader-test.txt