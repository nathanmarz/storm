# classloader_test

This is the subproject of storm, for testing the classloading merchanism
of Storm.

## Usage

To run the test, run the following command:
```bash
./run.sh
```

So you will something like the following at the end of the output:

```bash
=================================================================================
Storm::backtype.storm.topology.BasicBoltExecutor loaded by sun.misc.Launcher$AppClassLoader@1ef6a746
classloader_test::backtype.storm.testing.TestClassLoaderTopology$TestClassLoaderBolt loaded by backtype.storm.classloader.TopologyClassLoader@53ef9f1d
Storm::backtype.storm.testing.Foo loaded by sun.misc.Launcher$AppClassLoader@1ef6a746
classloader_test::backtype.storm.testing.Foo loaded by backtype.storm.classloader.TopologyClassLoader@53ef9f1d
=================================================================================
```

## License

Copyright © 2012 FIXME

Distributed under the Eclipse Public License, the same as Clojure.
