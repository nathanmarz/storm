#!/bin/bash
function quit {
    exit 1
}
trap quit 1 2 3 15  #Ctrl+C exits.

RELEASE=`head -1 project.clj | awk '{print $3}' | sed -e 's/\"//' | sed -e 's/\"//'`
LEIN=`which lein2 || which lein` 
export LEIN_ROOT=1

echo Making release $RELEASE

DIR=_release/storm-$RELEASE

rm -rf _release
rm -f *.zip 
$LEIN with-profile release clean
$LEIN with-profile release deps
$LEIN with-profile release jar
$LEIN with-profile release pom
mvn dependency:copy-dependencies

mkdir -p $DIR/lib
cp target/storm-*.jar $DIR/storm-${RELEASE}.jar
cp target/dependency/*.jar $DIR/lib
cp CHANGELOG.md $DIR/

echo $RELEASE > $DIR/RELEASE

mkdir -p $DIR/logback
mkdir -p $DIR/logs
cp -R logback/cluster.xml $DIR/logback/cluster.xml

mkdir $DIR/conf
cp conf/storm.yaml.example $DIR/conf/storm.yaml

cp -R src/ui/public $DIR/

cp -R bin $DIR/

cp README.markdown $DIR/
cp LICENSE.html $DIR/

cd _release
zip -r storm-$RELEASE.zip *
cd ..
mv _release/storm-*.zip .
rm -rf _release

