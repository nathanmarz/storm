#!/bin/bash

if [[ $# -gt 1 || "$1" == "-h" || "$1" == "--help" ]]; then
  echo "Usage: ${0##*/} [--dev]" >&2
  exit 1
fi

IS_DEV=""
if [ "$1" == "--dev" ]; then
  IS_DEV=1
fi

set -e -u -o pipefail -x

# Version, e.g. 0.8.1
RELEASE=`cat project.clj | sed '6q;d' | awk '{print $3}' | sed -e 's/\"//' | sed -e 's/\"//'`

rm -rf classes
rm -f *.jar
rm -f *.xml
time lein jar
time lein pom
if [ ! "$IS_DEV" ]; then
  scp storm*jar pom.xml clojars@clojars.org:
fi

rm -f *.jar
rm -rf classes
rm conf/log4j.properties
time lein jar
mv pom.xml old-pom.xml
sed 's/artifactId\>storm/artifactId\>storm-lib/g' old-pom.xml > pom.xml
  mv storm-$RELEASE.jar storm-lib-$RELEASE.jar
if [ ! "$IS_DEV" ]; then
  scp storm*jar pom.xml clojars@clojars.org:
  rm *xml
  rm *jar
fi
git checkout conf/log4j.properties

if [ "$IS_DEV" ]; then
  mvn install:install-file -Dfile=storm-lib-$RELEASE.jar -DpomFile=pom.xml \
    -DartifactId=storm-lib -DgroupId=storm -Dversion=$RELEASE -Dpackaging=jar \
    -DgeneratePom=false
fi

