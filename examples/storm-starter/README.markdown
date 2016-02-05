# Example Storm Topologies

Learn to use Storm!

---

Table of Contents

* <a href="#getting-started">Getting started</a>
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

    $ git clone git://github.com/apache/storm.git && cd storm/examples/storm-starter


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
[Storm project page](http://storm.apache.org).


<a name="maven"></a>

# Using storm-starter with Maven

## Install Maven

Install [Maven](http://maven.apache.org/) (preferably version 3.x) by following
the [Maven installation instructions](http://maven.apache.org/download.cgi).


## Build and install Storm jars locally

If you are using the latest development version of Storm, e.g. by having cloned the Storm git repository,
then you must first perform a local build of Storm itself.  Otherwise you will run into Maven errors such as
"Could not resolve dependencies for project `org.apache.storm:storm-starter:<storm-version>-SNAPSHOT`".

    # Must be run from the top-level directory of the Storm code repository
    $ mvn clean install -DskipTests=true

This command will build Storm locally and install its jar files to your user's `$HOME/.m2/repository/`.  When you run
the Maven command to build and run storm-starter (see below), Maven will then be able to find the corresponding version
of Storm in this local Maven repository at `$HOME/.m2/repository`.


## Packaging storm-starter for use on a Storm cluster

You can package a jar suitable for submitting to a Storm cluster with the command:

    $ mvn package

This will package your code and all the non-Storm dependencies into a single "uberjar" (or "fat jar") at the path
`target/storm-starter-{version}.jar`.

Example filename of the uberjar:

    >>> target/storm-starter-0.9.3-incubating-SNAPSHOT.jar

You can submit (run) a topology contained in this uberjar to Storm via the `storm` CLI tool:

    # Example 1: Run the ExclamationTopology in local mode (LocalCluster)
    $ storm jar target/storm-starter-*.jar org.apache.storm.starter.ExclamationTopology

    # Example 2: Run the RollingTopWords in remote/cluster mode,
    #            under the name "production-topology"
    $ storm jar storm-starter-*.jar org.apache.storm.starter.RollingTopWords production-topology remote

With submitting you can run topologies which use multilang, for example, `WordCountTopology`.

_Submitting a topology in local vs. remote mode:_
It depends on the actual code of a topology how you can or even must tell Storm whether to run the topology locally (in
an in-memory LocalCluster instance of Storm) or remotely (in a "real" Storm cluster).  In the case of
[RollingTopWords](src/jvm/storm/starter/RollingTopWords.java), for instance, this can be done by passing command line
arguments.
Topologies other than `RollingTopWords` -- such as [ExclamationTopology](src/jvm/storm/starter/ExclamationTopology.java)
-- may behave differently, e.g. by always submitting to a remote cluster (i.e. hardcoded in a way that you, as a user,
cannot change without modifying the topology code), or by requiring a customized configuration file that the topology
code will parse prior submitting the topology to Storm.  Similarly, further options such as the name of the topology may
be user-configurable or be hardcoded into the topology code.  So make sure you understand how the topology of your
choice is set up and configured!


## Running unit tests

Use the following Maven command to run the unit tests that ship with storm-starter.  Unfortunately `lein test` does not
yet run the included unit tests.

    $ mvn test


<a name="intellij-idea"></a>

# Using storm-starter with IntelliJ IDEA

## Importing storm-starter as a project in IDEA

The following instructions will import storm-starter as a new project in IntelliJ IDEA.


* Open _File > Import Project..._ and navigate to the storm-starter directory of your storm clone (e.g.
  `~/git/storm/examples/storm-starter`).
* Select _Import project from external model_, select "Maven", and click _Next_.
* In the following screen, enable the checkbox _Import Maven projects automatically_.  Leave all other values at their
  defaults.  Click _Next_.
* Make sure to select the *intellij* profile in the profiles screen.  This is important for making sure dependencies set correctly. 
* Click _Next_ on the following screen about selecting Maven projects to import.
* Select the JDK to be used by IDEA for storm-starter, then click _Next_.
    * At the time of this writing you should use JDK 7 and above.
    * It is strongly recommended to use Oracle JDK rather than OpenJDK.
* You may now optionally change the name of the project in IDEA.  The default name suggested by IDEA is "storm-starter".
  Click _Finish_ once you are done.
