#!/bin/bash

RELEASE=`head -1 project.clj | awk '{print $3}' | sed -e 's/\"//' | sed -e 's/\"//'`

rm *xml
sh bin/build_jar.sh
rm storm-$RELEASE.jar
mv storm-$RELEASE-release.jar storm-$RELEASE.jar
sed s/RELEASE/$RELEASE/g bin/release-pom.xml > pom.xml
# scp storm-$RELEASE.jar pom.xml clojars@clojars.org:

# rm conf/log4j.properties
# sh bin/build_jar.sh
# mv storm-$RELEASE-release.jar storm-lib-$RELEASE.jar
# mv pom.xml old-pom.xml
# sed 's/artifactId\>storm/artifactId\>storm-lib/g' old-pom.xml > pom.xml
# scp storm*jar pom.xml clojars@clojars.org:
# rm *xml
# rm *jar
# git checkout conf/log4j.properties
