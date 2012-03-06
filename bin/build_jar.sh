#!/bin/bash

RELEASE=`head -1 project.clj | awk '{print $3}' | sed -e 's/\"//' | sed -e 's/\"//'`

rm *jar
rm -rf classes
rm -rf lib
lein uberjar
java -jar lib/dev/jarjar-1.2.jar process bin/jarjar.rules storm-$RELEASE-standalone.jar storm-$RELEASE-release.jar
