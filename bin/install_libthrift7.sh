#!/bin/sh
mvn dependency:get -DgroupId=org.apache.thrift -DartifactId=libthrift -Dversion=0.7.0
mvn dependency:copy -Dartifact=org.apache.thrift:libthrift:0.7.0 -DoutputDirectory=./target
mvn dependency:copy -Dartifact=com.googlecode.jarjar:jarjar:1.3 -DoutputDirectory=./target

cat > ./target/libthrift.jj <<EOF
rule org.apache.thrift.** org.apache.thrift7.@1
EOF

java -jar ./target/jarjar-1.3.jar process ./target/libthrift.jj ./target/libthrift-0.7.0.jar ./target/libthrift7-0.7.0.jar

cat > ./target/libthrift7-pom.xml <<EOF
<?xml version="1.0" encoding="UTF-8"?>
<project xsi:schemaLocation="http://maven.apache.org/POM/4.0.0 http://maven.apache.org/xsd/maven-4.0.0.xsd" xmlns="http://maven.apache.org/POM/4.0.0"
    xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance">
  <modelVersion>4.0.0</modelVersion>
  <groupId>org.apache.storm</groupId>
  <artifactId>libthrift7</artifactId>
  <version>0.7.0</version>
  <name>Apache Thrift</name>
  <description>Thrift is a software framework for scalable cross-language services development.</description>
  <url>http://thrift.apache.org</url>
  <licenses>
    <license>
      <name>The Apache Software License, Version 2.0</name>
      <url>http://www.apache.org/licenses/LICENSE-2.0.txt"</url>
    </license>
  </licenses>
  <developers>
    <developer>
      <id>mcslee</id>
      <name>Mark Slee</name>
    </developer>
    <developer>
      <id>dreiss</id>
      <name>David Reiss</name>
    </developer>
    <developer>
      <id>aditya</id>
      <name>Aditya Agarwal</name>
    </developer>
    <developer>
      <id>marck</id>
      <name>Marc Kwiatkowski</name>
    </developer>
    <developer>
      <id>jwang</id>
      <name>James Wang</name>
    </developer>
    <developer>
      <id>cpiro</id>
      <name>Chris Piro</name>
    </developer>
    <developer>
      <id>bmaurer</id>
      <name>Ben Maurer</name>
    </developer>
    <developer>
      <id>kclark</id>
      <name>Kevin Clark</name>
    </developer>
    <developer>
      <id>jake</id>
      <name>Jake Luciani</name>
    </developer>
    <developer>
      <id>bryanduxbury</id>
      <name>Bryan Duxbury</name>
    </developer>
    <developer>
      <id>esteve</id>
      <name>Esteve Fernandez</name>
    </developer>
    <developer>
      <id>todd</id>
      <name>Todd Lipcon</name>
    </developer>
    <developer>
      <id>geechorama</id>
      <name>Andrew McGeachie</name>
    </developer>
    <developer>
      <id>molinaro</id>
      <name>Anthony Molinaro</name>
    </developer>
    <developer>
      <id>roger</id>
      <name>Roger Meier</name>
    </developer>
    <developer>
      <id>jfarrell</id>
      <name>Jake Farrell</name>
    </developer>
  </developers>
  <scm>
    <connection>scm:svn:http://svn.apache.org/repos/asf/thrift/trunk/</connection>
    <developerConnection>scm:svn:http://svn.apache.org/repos/asf/thrift/trunk/</developerConnection>
    <url>http://svn.apache.org/viewcvs.cgi/thrift</url>
  </scm>
  <dependencies>
    <dependency>
      <groupId>org.slf4j</groupId>
      <artifactId>slf4j-api</artifactId>
      <version>1.5.8</version>
    </dependency>
    <dependency>
      <groupId>commons-lang</groupId>
      <artifactId>commons-lang</artifactId>
      <version>2.5</version>
    </dependency>
    <dependency>
      <groupId>javax.servlet</groupId>
      <artifactId>servlet-api</artifactId>
      <version>2.5</version>
    </dependency>
    <dependency>
      <groupId>org.apache.httpcomponents</groupId>
      <artifactId>httpclient</artifactId>
      <version>4.0.1</version>
    </dependency>
  </dependencies>
</project>

EOF

mvn install:install-file -Dfile=./target/libthrift7-0.7.0.jar -DpomFile=./target/libthrift7-pom.xml