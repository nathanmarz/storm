#!/bin/bash

set -e  # die if anything bad happens

function banner {
  echo ; echo "------------- $1 ---------------" ; echo
}

LEIN=`which lein2 || which lein`
export LEIN_ROOT=1


for module in $(cat MODULES)
do
  banner "Building $module"
  cd $module
  if [ $module != "storm-console-logging" ]
  then
    rm -f ../conf/logback.xml
  else
    git checkout ../conf/logback.xml ; true
  fi

  $LEIN with-profile release clean
  $LEIN with-profile release deps
  $LEIN with-profile release jar
  $LEIN with-profile release install
  $LEIN with-profile release pom

  git checkout ../conf/logback.xml ; true
  cd ..
done
