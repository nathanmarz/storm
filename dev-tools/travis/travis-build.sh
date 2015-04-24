#!/bin/bash
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.

PYTHON_VERSION_TO_FILE=`python -V > /tmp/python_version 2>&1`
PYTHON_VERSION=`cat /tmp/python_version`
RUBY_VERSION=`ruby -v`
NODEJS_VERSION=`node -v`

echo "Python version : $PYTHON_VERSION"
echo "Ruby version : $RUBY_VERSION"
echo "NodeJs version : $NODEJS_VERSION"


STORM_SRC_ROOT_DIR=$1

TRAVIS_SCRIPT_DIR=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )

cd ${STORM_SRC_ROOT_DIR}

# Travis CI doesn't allow stdout bigger than 4M, so we have to reduce log while running tests
export LOG_LEVEL=WARN
# We should concern that Travis CI could be very slow cause it uses VM
export STORM_TEST_TIMEOUT_MS=100000

# We now lean on Travis CI's implicit behavior, ```mvn clean install -DskipTests``` before running script
mvn test -fae

BUILD_RET_VAL=$?

if [ ${BUILD_RET_VAL} -ne 0 ]
then
    echo "There may be clojure test errors from storm-core, printing error reports..."
    python ${TRAVIS_SCRIPT_DIR}/print-errors-from-clojure-test-reports.py ${STORM_SRC_ROOT_DIR}/storm-core/target/test-reports
else
    echo "Double checking clojure test-report, Errors or Failures:"
    egrep -il '<fail|<error' */target/test-reports/*.xml | xargs -I '{}' bash -c "echo -n '{}':' '; egrep -ic '<fail|<error' '{}'"
    egrep -iq '<fail|<error' */target/test-reports/*.xml
fi

exit ${BUILD_RET_VAL}