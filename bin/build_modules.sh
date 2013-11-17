#!/bin/bash

LEIN=`which lein2 || which lein` 
export LEIN_ROOT=1


for module in $(cat MODULES)
do
	echo "Building $module"
	cd $module
	if [ $module != "storm-console-logging" ]
		then
			rm ../conf/logback.xml
	fi


	$LEIN with-profile release clean
	$LEIN with-profile release deps
	$LEIN with-profile release jar
	$LEIN with-profile release install
	$LEIN with-profile release pom

	git checkout ../conf/logback.xml
	cd ..
done