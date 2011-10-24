rm -rf gen-javabean gen-py py
rm -rf jvm/backtype/storm/generated
thrift7 --gen java:beans,hashcode,nocamel --gen py:utf8strings storm.thrift
mv gen-javabean/backtype/storm/generated jvm/backtype/storm/generated
mv gen-py py
rm -rf gen-javabean
