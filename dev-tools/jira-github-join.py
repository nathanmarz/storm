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

from optparse import OptionParser
from datetime import datetime
from github import GitHub
from jira import JiraRepo
from report.report_builder import CompleteReportBuilder


def main():
    parser = OptionParser(usage="usage: %prog [options]")
    parser.add_option("-g", "--github-user", dest="gituser",
                      type="string", help="github User, if not supplied no auth is used", metavar="USER")

    (options, args) = parser.parse_args()

    jira_repo = JiraRepo("https://issues.apache.org/jira/rest/api/2")
    github_repo = GitHub(options)

    print "Report generated on: %s (GMT)" % (datetime.strftime(datetime.utcnow(), "%Y-%m-%d %H:%M:%S"))

    report_builder = CompleteReportBuilder(jira_repo, github_repo)
    report_builder.report.print_all()


if __name__ == "__main__":
    main()
