mkdir -p doc
javadoc -d doc-$1/ `find src -name "*.java" | grep -v generated`
