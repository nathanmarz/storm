mkdir -p doc
javadoc -d doc/ `find src -name "*.java" | grep -v generated`
