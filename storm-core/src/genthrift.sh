#!/usr/bin/env bash
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

rm -rf gen-javabean gen-py py
rm -rf jvm/org/apache/storm/generated
thrift --gen java:beans,hashcode,nocamel,generated_annotations=undated --gen py:utf8strings storm.thrift
for file in gen-javabean/org/apache/storm/generated/* ; do
  cat java_license_header.txt ${file} > ${file}.tmp
  mv -f ${file}.tmp ${file}
done
cat py_license_header.txt gen-py/__init__.py > gen-py/__init__.py.tmp
mv gen-py/__init__.py.tmp gen-py/__init__.py
for file in gen-py/storm/* ; do
  cat py_license_header.txt ${file} > ${file}.tmp
  mv -f ${file}.tmp ${file}
done
mv gen-javabean/org/apache/storm/generated jvm/org/apache/storm/generated
mv gen-py py
rm -rf gen-javabean
