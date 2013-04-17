mkdir -p doc
javadoc -d doc-$1/ `find . -name "*.java" | grep -v generated`
