#!/bin/bash 
function quit {
    exit 1
}
trap quit 1 2 3 15  #Ctrl+C exits.

RELEASE=`head -1 project.clj | awk '{print $3}' | sed -e 's/\"//' | sed -e 's/\"//'`
LEIN=`which lein2 || which lein`

echo ==== Storm Jar ====
$LEIN with-profile release clean
$LEIN with-profile release jar
$LEIN with-profile release pom
scp target/storm*jar pom.xml clojars@clojars.org:
rm -Rf target *.xml

echo ==== Storm-Lib Jar ====
rm conf/logback.xml
$LEIN with-profile lib clean
$LEIN with-profile lib jar
$LEIN with-profile lib pom
sed -i '' -e 's/artifactId\>storm/artifactId\>storm-lib/g' pom.xml
mv target/storm-$RELEASE.jar target/storm-lib-$RELEASE.jar
scp target/storm*jar pom.xml clojars@clojars.org:
rm -Rf target *.xml

git checkout conf/logback.xml
