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
=======================================================================
Storm::backtype.storm.testing.Foo loaded by sun.misc.Launcher$AppClassLoader@425224ee
classloader_test::backtype.storm.testing.Foo loaded by backtype.storm.classloader.TopologyClassLoader@31d520c4
=======================================================================
```

## License

Copyright © 2012 FIXME

Distributed under the Eclipse Public License, the same as Clojure.
