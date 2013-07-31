storm-kafka-0.8-plus
====================

Port of storm-kafka to support kafka >= 0.8

## Installing kafka 0.8 into local maven repository

Unfortunately the current beta pom is slightly broken ([KAFKA-974](https://issues.apache.org/jira/browse/KAFKA-974)) so you might want to build your own

- checkout kafka 0.8 branch
- Check [KAFKA-939](https://issues.apache.org/jira/browse/KAFKA-939) for instructions on how to handle javadoc erros when publishing
- GPG:
    - ```gpg2 --gen-key```
    - ```cp ~/.gnupg/secring.gpg ~/.sbt/gpg/secring.asc```
- publish kafka to local ivy repository
    - ```./sbt "++2.9.2" clean package publish-local```
- publish kafka to local maven repo
    - ```mvn install:install-file -Dfile=$HOME/.ivy2/local/org.apache.kafka/kafka_2.9.2/0.8.0-beta1/jars/kafka_2.9.2.jar -DpomFile=$HOME/.ivy2/local/org.apache.kafka/kafka_2.9.2/0.8.0-beta1/poms/kafka_2.9.2.pom```

