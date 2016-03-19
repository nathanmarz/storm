---
layout: documentation
---
This page outlines how to set up a Storm project for development. The steps are:

1. Add Storm jars to classpath
2. If using multilang, add multilang dir to classpath

Follow along to see how to set up the [storm-starter](http://github.com/nathanmarz/storm-starter) project in Eclipse.

### Add Storm jars to classpath

You'll need the Storm jars on your classpath to develop Storm topologies. Using [Maven](Maven.html) is highly recommended. [Here's an example](https://github.com/nathanmarz/storm-starter/blob/master/m2-pom.xml) of how to setup your pom.xml for a Storm project. If you don't want to use Maven, you can include the jars from the Storm release on your classpath. 

[storm-starter](http://github.com/nathanmarz/storm-starter) uses [Leiningen](http://github.com/technomancy/leiningen) for build and dependency resolution. You can install leiningen by downloading [this script](https://raw.github.com/technomancy/leiningen/stable/bin/lein), placing it on your path, and making it executable. To retrieve the dependencies for Storm, simply run `lein deps` in the project root.

To set up the classpath in Eclipse, create a new Java project, include `src/jvm/` as a source path, and make sure all the jars in `lib/` and `lib/dev/` are in the `Referenced Libraries` section of the project.

### If using multilang, add multilang dir to classpath

If you implement spouts or bolts in languages other than Java, then those implementations should be under the `multilang/resources/` directory of the project. For Storm to find these files in local mode, the `resources/` dir needs to be on the classpath. You can do this in Eclipse by adding `multilang/` as a source folder. You may also need to add multilang/resources as a source directory.

For more information on writing topologies in other languages, see [Using non-JVM languages with Storm](Using-non-JVM-languages-with-Storm.html).

To test that everything is working in Eclipse, you should now be able to `Run` the `WordCountTopology.java` file. You will see messages being emitted at the console for 10 seconds.
