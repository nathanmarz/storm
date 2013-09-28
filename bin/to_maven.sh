#!/bin/bash 
function quit {
    exit 1
}
trap quit 1 2 3 15  #Ctrl+C exits.

RELEASE=`cat VERSION`
LEIN=`which lein2 || which lein` 
export LEIN_ROOT=1


sh bin/build_modules.sh

echo ==== Module jars ====
for module in $(cat MODULES)
do
	cd $module
	scp target/*jar pom.xml clojars@clojars.org:
	cd ..
done


#L

echo ==== Storm jar ====
$LEIN clean
$LEIN pom
$LEIN jar
scp pom.xml target/*jar clojars@clojars.org:

echo ==== Storm-lib jar ====
cd storm-lib
$LEIN clean
$LEIN pom
$LEIN jar
scp pom.xml target/*jar clojars@clojars.org:
cd ..
