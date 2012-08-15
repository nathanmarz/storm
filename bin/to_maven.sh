#!/bin/bash 
function quit {
    exit 1
}
trap quit 1 2 3 15  #Ctrl+C exits.

RELEASE=`head -1 project.clj | awk '{print $3}' | sed -e 's/\"//' | sed -e 's/\"//'`

rm -rf classes
rm -f *jar
rm -f *xml
lein jar
lein pom
scp storm*jar pom.xml clojars@clojars.org:

rm *jar
rm -rf classes
rm conf/logback.xml
lein jar

cp project.clj orig-project.clj
sed -i '' -e 's/\[.*logback[^]]*\]//g' project.clj
sed -i '' -e 's/\[.*log4j-over-slf4j[^]]*\]//g' project.clj

lein pom
mv orig-project.clj project.clj

mv pom.xml old-pom.xml
sed 's/artifactId\>storm/artifactId\>storm-lib/g' old-pom.xml > pom.xml
mv storm-$RELEASE.jar storm-lib-$RELEASE.jar
scp storm*jar pom.xml clojars@clojars.org:
rm *xml
rm *jar
git checkout conf/logback.xml


