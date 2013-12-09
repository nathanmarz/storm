#!/bin/bash 

# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
# http:# www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

function quit {
    exit 1
}
trap quit 1 2 3 15  #Ctrl+C exits.

RELEASE=`cat VERSION`
LEIN=`which lein2 || which lein` 
export LEIN_ROOT=1


sh bin/build_modules.sh

echo ==== Module jars ====
for module in $(cat MODULES)
do
	cd $module
	scp target/*jar pom.xml clojars@clojars.org:
	cd ..
done


#L

echo ==== Storm jar ====
$LEIN clean
$LEIN pom
$LEIN jar
scp pom.xml target/*jar clojars@clojars.org:

echo ==== Storm-lib jar ====
cd storm-lib
$LEIN clean
$LEIN pom
$LEIN jar
scp pom.xml target/*jar clojars@clojars.org:
cd ..
