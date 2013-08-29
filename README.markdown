# Example Storm topologies

storm-starter contains a variety of examples of using Storm.  If this is your first time working with Storm, check out
these topologies first:

1. [ExclamationTopology](src/jvm/storm/starter/ExclamationTopology.java):  Basic topology written in all Java
2. [WordCountTopology](src/jvm/storm/starter/WordCountTopology.java):  Basic topology that makes use of multilang by
   implementing one bolt in Python
3. [ReachTopology](src/jvm/storm/starter/ReachTopology.java): Example of complex DRPC on top of Storm

See the [Storm project page](http://github.com/nathanmarz/storm) for more information.


## Running an example with Leiningen

Install Leiningen by following the [leiningen installation instructions](https://github.com/technomancy/leiningen).
The storm-starter build uses Leiningen 2.0.


### To run a Java example:

    $ lein deps
    $ lein compile
    $ java -cp $(lein classpath) storm.starter.ExclamationTopology


### To run a Clojure example:

    $ lein deps
    $ lein compile
    $ lein run -m storm.starter.clj.word-count


## Maven

Maven is an alternative to Leiningen. storm-starter contains m2-pom.xml which can be used with Maven using the `-f`
option. For example, to compile and run `WordCountTopology` in local mode, use this command:


    $ mvn -f m2-pom.xml compile exec:java -Dexec.classpathScope=compile -Dexec.mainClass=storm.starter.WordCountTopology

You can package a jar suitable for submitting to a cluster with this command:

    $ mvn -f m2-pom.xml package

This will package your code and all the non-Storm dependencies into a single "uberjar" at the path
`target/storm-starter-{version}-jar-with-dependencies.jar`.


### To run unit tests

Use the following Maven command to run the unit tests that ship with storm-starter.  Unfortunately `lein test` does not
yet run the included unit tests.

    $ mvn -f m2-pom.xml test
