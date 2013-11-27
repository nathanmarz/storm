#!/bin/bash
function quit {
    exit 1
}
trap quit 1 2 3 15  #Ctrl+C exits.

RELEASE=`cat VERSION`
LEIN=`which lein2 || which lein` 
export LEIN_ROOT=1

echo Making release $RELEASE

DIR=`pwd`/_release/storm-$RELEASE

rm -rf _release
rm -f *.zip
rm -f *.tar.gz
$LEIN pom || exit 1
mkdir -p $DIR/lib


sh bin/build_modules.sh

for module in $(cat MODULES)
do
	cd $module
	mvn dependency:copy-dependencies || exit 1
	cp -f target/dependency/*.jar $DIR/lib/
	cp -f target/*.jar $DIR/
	cd ..
done

cd _release/storm-$RELEASE
for i in *.jar
do
	rm -f lib/$i
done 
cd ../..

cp CHANGELOG.md $DIR/

echo $RELEASE > $DIR/RELEASE

mkdir -p $DIR/logback
mkdir -p $DIR/logs
cp -R logback/cluster.xml $DIR/logback/cluster.xml

mkdir $DIR/conf
cp conf/storm.yaml.example $DIR/conf/storm.yaml

cp -R storm-core/src/ui/public $DIR/

cp -R bin $DIR/

cp README.markdown $DIR/
cp LICENSE.html $DIR/

cd _release
zip -r storm-$RELEASE.zip *
mv storm-*.zip ../
tar -cvzf ../storm-$RELEASE.tar.gz ./

cd ..

rm -rf _release

