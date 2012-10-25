#!/bin/bash

RELEASE=`cat project.clj | sed '6q;d' | awk '{print $3}' | sed -e 's/\"//' | sed -e 's/\"//'`

rm -rf classes
rm *jar
rm *xml
lein jar
lein pom
scp storm*jar pom.xml clojars@clojars.org:

rm *jar
rm -rf classes
rm conf/log4j.properties
lein jar
mv pom.xml old-pom.xml
sed 's/artifactId\>storm/artifactId\>storm-lib/g' old-pom.xml > pom.xml
mv storm-$RELEASE.jar storm-lib-$RELEASE.jar
scp storm*jar pom.xml clojars@clojars.org:
rm *xml
rm *jar
git checkout conf/log4j.properties
