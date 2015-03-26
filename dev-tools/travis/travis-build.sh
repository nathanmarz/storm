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

STORM_SRC_ROOT_DIR=$1

TRAVIS_SCRIPT_DIR=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )

cd ${STORM_SRC_ROOT_DIR}

# Travis CI doesn't allow stdout bigger than 4M, so we have to reduce log while running tests
export LOG_LEVEL=WARN
# We should concern that Travis CI could be very slow cause it uses VM
export export STORM_TEST_TIMEOUT_MS=100000

mvn clean test

BUILD_RET_VAL=$?

if [ ${BUILD_RET_VAL} -ne 0 ]
then
    echo "There may be clojure test errors from storm-core, printing error reports..."
    python ${TRAVIS_SCRIPT_DIR}/print-errors-from-clojure-test-reports.py ${STORM_SRC_ROOT_DIR}/storm-core/target/test-reports
fi

exit ${BUILD_RET_VAL}