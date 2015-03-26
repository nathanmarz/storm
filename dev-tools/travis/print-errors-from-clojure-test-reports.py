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
from xml.etree.ElementTree import ElementTree


def print_error_reports_from_report_file(file_path):
    tree = ElementTree()
    tree.parse(file_path)

    testcases = tree.findall(".//testcase")
    for testcase in testcases:
        error = testcase.find("error")
        if error is not None:
            print "-" * 50
            print "classname: %s / testname: %s" % (testcase.get("classname"), testcase.get("name"))
            print error.text
            print "-" * 50


def main(report_dir_path):
    for test_report in glob.iglob(report_dir_path + '/*.xml'):
        file_path = os.path.abspath(test_report)
        try:
            print_error_reports_from_report_file(file_path)
        except Exception, e:
            print "Error while reading report file, %s" % file_path
            print "Exception: %s" % e


if __name__ == "__main__":
    if sys.argv < 2:
        print "Usage: %s [report dir path]" % sys.argv[0]
        sys.exit(1)

    main(sys.argv[1])