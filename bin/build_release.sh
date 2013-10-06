#!/bin/bash
function quit {
  exit 1
}
trap quit 1 2 3 15  # Ctrl+C exits.

function banner {
  echo ; echo "=========================" ; echo "==" ; echo "== $1" ; echo "==" ; echo
}

RELEASE=`cat VERSION`
LEIN=`which lein2 || which lein`
export LEIN_ROOT=1

# ==========================================================================
banner "Making release $RELEASE"

CODE_ROOT=`pwd`
DIR="$CODE_ROOT/_release/storm-$RELEASE"
ZIPFILE="$CODE_ROOT/storm-$RELEASE.zip"

rm -rf "$DIR"
rm -f  "$ZIPFILE"
$LEIN pom || exit 1
mkdir -p "$DIR/lib"

# ==========================================================================
banner "Building submodules"

/bin/bash "$CODE_ROOT/bin/build_modules.sh" || exit 1

# ==========================================================================
banner "Gathering dependencies"

for module in $(cat MODULES)
do
  cd "$CODE_ROOT/$module"
  mvn dependency:copy-dependencies || exit 1
  cp -f target/dependency/*.jar "$DIR/lib/"
  cp -f target/*.jar "$DIR/"
  cd "$CODE_ROOT"
done

# The netty libs have storm itself as a dependency; remove any jar in $DIR/lib/ that is in $DIR/
cd "$DIR"
for base_jar in *.jar
do
  rm -f lib/$base_jar
done
cd "$CODE_ROOT"

# ==========================================================================
banner "Copying support files"

cp -v CHANGELOG.md "$DIR/"

echo $RELEASE > "$DIR/RELEASE"

mkdir -p "$DIR/logback"
mkdir -p "$DIR/logs"
cp -vR logback/cluster.xml "$DIR/logback/cluster.xml"

mkdir "$DIR/conf"
cp -v conf/storm.yaml.example "$DIR/conf/storm.yaml"

cp -vR storm-core/src/ui/public "$DIR/"

cp -vRp bin "$DIR/"
rm "$DIR"/bin/build_{release,modules}.sh

cp -v README.markdown "$DIR/"
cp -v LICENSE.html "$DIR/"

# ==========================================================================
banner "Building Zip File in '$ZIPFILE'"

cd "$CODE_ROOT/_release"
zip -r "$ZIPFILE" *
cd "$CODE_ROOT"
echo

if [ "$STORM_KEEP_RELEASE" == "true" ] ; then
  echo "keeping _release dir '$DIR'"
else
  echo "removing _release dir"
  rm -rf "$DIR"
fi
