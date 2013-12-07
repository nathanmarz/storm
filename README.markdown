# Example Storm Topologies

Learn to use Storm!

---

Table of Contents

* <a href="#getting-started">Getting started</a>
* <a href="#leiningen">Using storm-starter with Leiningen</a>
* <a href="#maven">Using storm-starter with Maven</a>
* <a href="#intellij-idea">Using storm-starter with IntelliJ IDEA</a>

---


<a name="getting-started"></a>

# Getting started

## Prerequisites

First, you need `java` and `git` installed and in your user's `PATH`.  Also, two of the examples in storm-starter
require Python and Ruby.

Next, make sure you have the storm-starter code available on your machine.  Git/GitHub beginners may want to use the
following command to download the latest storm-starter code and change to the new directory that contains the downloaded
code.

    $ git clone git://github.com/nathanmarz/storm-starter.git && cd storm-starter


## storm-starter overview

storm-starter contains a variety of examples of using Storm.  If this is your first time working with Storm, check out
these topologies first:

1. [ExclamationTopology](src/jvm/storm/starter/ExclamationTopology.java):  Basic topology written in all Java
2. [WordCountTopology](src/jvm/storm/starter/WordCountTopology.java):  Basic topology that makes use of multilang by
   implementing one bolt in Python
3. [ReachTopology](src/jvm/storm/starter/ReachTopology.java): Example of complex DRPC on top of Storm

After you have familiarized yourself with these topologies, take a look at the other topopologies in
[src/jvm/storm/starter/](src/jvm/storm/starter/) such as [RollingTopWords](src/jvm/storm/starter/RollingTopWords.java)
for more advanced implementations.

If you want to learn more about how Storm works, please head over to the
[Storm project page](http://github.com/nathanmarz/storm).


<a name="leiningen"></a>

# Using storm-starter with Leiningen

## Install Leiningen

The storm-starter build uses [Leiningen](http://leiningen.org/) 2.0.  Install Leiningen by following the
[leiningen installation instructions](https://github.com/technomancy/leiningen).


## Running topologies with Leiningen

### To run a Java topology

    $ lein deps
    $ lein compile
    $ java -cp $(lein classpath) storm.starter.ExclamationTopology


### To run a Clojure topology:

    $ lein deps
    $ lein compile
    $ lein run -m storm.starter.clj.word-count


<a name="maven"></a>

# Using storm-starter with Maven

## Install Maven

[Maven](http://maven.apache.org/) is an alternative to Leiningen.  Install Maven (preferably version 3.x) by following
the [Maven installation instructions](http://maven.apache.org/download.cgi).


## Running topologies with Maven

storm-starter contains [m2-pom.xml](m2-pom.xml) which can be used with Maven using the `-f` option. For example, to
compile and run `WordCountTopology` in local mode, use the command:

    $ mvn -f m2-pom.xml compile exec:java -Dstorm.topology=storm.starter.WordCountTopology

You can also run clojure topologies with Maven:

    $ mvn -f m2-pom.xml compile exec:java -Dstorm.topology=storm.starter.clj.word_count

## Packaging storm-starter for use on a Storm cluster

You can package a jar suitable for submitting to a Storm cluster with the command:

    $ mvn -f m2-pom.xml package

This will package your code and all the non-Storm dependencies into a single "uberjar" at the path
`target/storm-starter-{version}-jar-with-dependencies.jar`.


## Running unit tests

Use the following Maven command to run the unit tests that ship with storm-starter.  Unfortunately `lein test` does not
yet run the included unit tests.

    $ mvn -f m2-pom.xml test


<a name="intellij-idea"></a>

# Using storm-starter with IntelliJ IDEA

## Importing storm-starter as a project in IDEA

The following instructions will import storm-starter as a new project in IntelliJ IDEA.

* Copy `m2-pom.xml` to `pom.xml`.  This is requried so that IDEA (or Eclipse) can properly detect the maven
  configuration.
* Open _File > Import Project..._ and navigate to the top-level directory of your storm-starter clone (e.g.
  `~/git/storm-starter`).
* Select _Import project from external model_, select "Maven", and click _Next_.
* In the following screen, enable the checkbox _Import Maven projects automatically_.  Leave all other values at their
  defaults.  Click _Next_.
* Click _Next_ on the following screen about selecting Maven projects to import.
* Select the JDK to be used by IDEA for storm-starter, then click _Next_.
    * At the time of this writing you should use JDK 6.
    * It is strongly recommended to use Sun/Oracle JDK 6 rather than OpenJDK 6.
* You may now optionally change the name of the project in IDEA.  The default name suggested by IDEA is "storm-starter".
  Click _Finish_ once you are done.
