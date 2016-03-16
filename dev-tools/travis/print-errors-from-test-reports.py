#!/usr/bin/python
# -*- coding: utf-8 -*-
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

import os
import sys
import glob
import traceback
from xml.etree.ElementTree import ElementTree

def print_detail_information(testcase, fail_or_error):
    print "-" * 50
    print "classname: %s / testname: %s" % (testcase.get("classname"), testcase.get("name"))
    print fail_or_error.text
    stdout = testcase.find("system-out")
    if stdout != None:
        print "-" * 20, "system-out", "-"*20
        print stdout.text
    stderr = testcase.find("system-err")
    if stderr != None:
        print "-" * 20, "system-err", "-"*20
        print stderr.text
    print "-" * 50


def print_error_reports_from_report_file(file_path):
    tree = ElementTree()
    try:
        tree.parse(file_path)
    except:
        print "-" * 50
        print "Error parsing %s"%file_path
        f = open(file_path, "r");
        print f.read();
        print "-" * 50
        return

    testcases = tree.findall(".//testcase")
    for testcase in testcases:
        error = testcase.find("error")
        if error is not None:
            print_detail_information(testcase, error)

        fail = testcase.find("fail")
        if fail is not None:
            print_detail_information(testcase, fail)

        failure = testcase.find("failure")
        if failure is not None:
            print_detail_information(testcase, failure)


def main(report_dir_path):
    for test_report in glob.iglob(report_dir_path + '/*.xml'):
        file_path = os.path.abspath(test_report)
        try:
            print "Checking %s" % test_report
            print_error_reports_from_report_file(file_path)
        except Exception, e:
            print "Error while reading report file, %s" % file_path
            print "Exception: %s" % e
            traceback.print_exc()


if __name__ == "__main__":
    if sys.argv < 2:
        print "Usage: %s [report dir path]" % sys.argv[0]
        sys.exit(1)

    main(sys.argv[1])
