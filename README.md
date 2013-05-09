storm-kafka-0.8-plus
====================

Port of storm-kafka to support kafka >= 0.8

I'm new to storm and kafka, so this is very much work in progress.

## Installing kafka 0.8 into local maven repository

Until kafka 0.8 has been release you need to install kafka 0.8 into you local repository if you want to use it from maven

- checkout kafka 0.8 branch

- publish kafka to local ivy repository

- ```
./sbt "++2.9.2" clean package publish-local
```

- publish kafka to local maven repo

- ```
mvn install:install-file -Dfile=$HOME/.ivy2/local/org.apache/kafka_2.9.2/0.8.0-SNAPSHOT/jars/kafka_2.9.2.jar -DpomFile=$HOME/.ivy2/local/org.apache/kafka_2.9.2/0.8.0-SNAPSHOT/poms/kafka_2.9.2.pom
```

