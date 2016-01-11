#!/bin/sh -x
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
set -u
set -e

BASE="$1"
find "$BASE" -type f -print0 | xargs -0 egrep -l 'backtype.storm|storm.trident|storm.starter|storm.kafka|"backtype", "storm"' | egrep -v '.git/|docs/|CHANGELOG.md|dev-tools/move_package.sh|StormShadeRequest.java' | xargs sed -i.back -e 's/storm\(.\)trident/org\1apache\1storm\1trident/g' -e 's/backtype\(.\)storm/org\1apache\1storm/g' -e 's/storm\([\.\\]\)starter/org\1apache\1storm\1starter/g' -e 's/storm\([\.\\]\)kafka/org\1apache\1storm\1kafka/g' -e 's/"backtype", "storm"/"org", "apache", "storm"/g'
find "$BASE" -iname \*.back | xargs rm
mkdir -p "$BASE"/storm-core/src/jvm/org/apache/storm/ "$BASE"/storm-core/src/clj/org/apache/storm/ "$BASE"/storm-core/test/jvm/org/apache/storm/ "$BASE"/storm-core/test/clj/org/apache/storm/ "$BASE"/storm-core/test/clj/integration/org/apache/storm/
#STORM-CORE
#SRC JVM
git mv "$BASE"/storm-core/src/jvm/backtype/storm/* "$BASE"/storm-core/src/jvm/org/apache/storm/
rm -rf "$BASE"/storm-core/src/jvm/backtype
git mv "$BASE"/storm-core/src/jvm/storm/trident "$BASE"/storm-core/src/jvm/org/apache/storm
rm -rf "$BASE"/storm-core/src/jvm/storm

#SRC CLJ
git mv "$BASE"/storm-core/src/clj/backtype/storm/* "$BASE"/storm-core/src/clj/org/apache/storm/
rm -rf "$BASE"/storm-core/src/clj/backtype
git mv "$BASE"/storm-core/src/clj/storm/trident "$BASE"/storm-core/src/clj/org/apache/storm
rm -rf "$BASE"/storm-core/src/clj/storm

#TEST JVM
git mv "$BASE"/storm-core/test/jvm/backtype/storm/* "$BASE"/storm-core/test/jvm/org/apache/storm/
rm -rf "$BASE"/storm-core/test/jvm/backtype
#git mv "$BASE"/storm-core/test/jvm/storm/trident "$BASE"/storm-core/test/jvm/org/apache/storm
#rm -rf "$BASE"/storm-core/test/jvm/storm

#TEST CLJ
git mv "$BASE"/storm-core/test/clj/backtype/storm/* "$BASE"/storm-core/test/clj/org/apache/storm/
rm -rf "$BASE"/storm-core/test/clj/backtype
git mv "$BASE"/storm-core/test/clj/storm/trident "$BASE"/storm-core/test/clj/org/apache/storm
rm -rf "$BASE"/storm-core/test/clj/storm
git mv "$BASE"/storm-core/test/clj/integration/storm/* "$BASE"/storm-core/test/clj/integration/org/apache/storm
rm -rf "$BASE"/storm-core/test/clj/integration/storm
git mv "$BASE"/storm-core/test/clj/integration/backtype/storm/* "$BASE"/storm-core/test/clj/integration/org/apache/storm
rm -rf "$BASE"/storm-core/test/clj/integration/backtype

#STORM-STARTER
mkdir -p "$BASE"/examples/storm-starter/src/jvm/org/apache/ "$BASE"/examples/storm-starter/src/clj/org/apache/ "$BASE"/examples/storm-starter/test/jvm/org/apache/
#SRC JVM
git mv "$BASE"/examples/storm-starter/src/jvm/storm "$BASE"/examples/storm-starter/src/jvm/org/apache/

#SRC CLJ
git mv "$BASE"/examples/storm-starter/src/clj/storm "$BASE"/examples/storm-starter/src/clj/org/apache/

#TEST JVM
git mv "$BASE"/examples/storm-starter/test/jvm/storm "$BASE"/examples/storm-starter/test/jvm/org/apache/


#STORM-KAFKA
mkdir -p "$BASE"/external/storm-kafka/src/jvm/org/apache/ "$BASE"/external/storm-kafka/src/test/org/apache/

#SRC JVM
git mv "$BASE"/external/storm-kafka/src/jvm/storm "$BASE"/external/storm-kafka/src/jvm/org/apache/

#TEST JVM
git mv "$BASE"/external/storm-kafka/src/test/storm "$BASE"/external/storm-kafka/src/test/org/apache/

