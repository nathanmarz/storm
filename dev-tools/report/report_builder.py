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
from report import CompleteReport, GitHubReport, JiraReport, JiraGitHubCombinedReport


class ReportBuilder:
    def __init__(self, jira_repo=None, github_repo=None):
        self.jira_repo = jira_repo
        self.github_repo = github_repo

    def build(self):
        pass


class CompleteReportBuilder(ReportBuilder):
    def __init__(self, jira_repo=None, github_repo=None):
        ReportBuilder.__init__(self, jira_repo, github_repo)
        self.report = CompleteReport()
        self.build()

    def build(self):
        # all open github pull requests
        github_open = GitHubReport(self.github_repo.open_pulls("apache", "storm"))
        github_bad_jira = GitHubReport(None, "\nGITHUB PULL REQUESTS WITH BAD OR CLOSED JIRA ID")
        github_without_jira = GitHubReport(None, "\nGITHUB PULL REQUESTS WITHOUT A JIRA ID")
        github_unresolved_jira = GitHubReport(None, "\nGITHUB PULL REQUESTS WITH UNRESOLVED JIRA ID")
        github_unresolved_jira_voted = GitHubReport(None, "\nGITHUB PULL REQUESTS WITH VOTES FOR UNRESOLVED JIRAS")
        github_open_jira = GitHubReport(None, "\nGITHUB PULL REQUESTS WITH OPEN JIRA ID")
        github_unresolved_not_open_jira = GitHubReport(None, "\nGITHUB PULL REQUESTS WITH UNRESOLVED BUT NOT OPEN JIRA ID")

        # all unresolved JIRA issues
        jira_unresolved = JiraReport(self.jira_repo.unresolved_jiras("STORM"))
        jira_open = JiraReport(dict((x, y) for x, y in self.jira_repo.unresolved_jiras("STORM").items() if y.get_status() == 'Open'))
        jira_in_progress = JiraReport(dict((x, y) for x, y in self.jira_repo.in_progress_jiras("STORM").items() if y.get_status() == 'In Progress'),
                                      "\nIN PROGRESS JIRA ISSUES")

        for pull in github_open.pull_requests:
            if pull.has_jira_id():
                pull_jira_id = pull.jira_id()
                if pull_jira_id not in jira_unresolved.issues:
                    github_bad_jira.pull_requests.append(pull)
                else:
                    github_unresolved_jira.pull_requests.append(pull)
                    if jira_unresolved.issues[pull_jira_id].has_voted_comment():
                        github_unresolved_jira_voted.pull_requests.append(pull)
                    if pull_jira_id in jira_open.issues:
                        github_open_jira.pull_requests.append(pull)
                    else:
                        github_unresolved_not_open_jira.pull_requests.append(pull)
            else:
                github_without_jira.pull_requests.append(pull)

        jira_github_open = JiraGitHubCombinedReport(jira_open, github_open_jira,
                                                    "\nOPEN JIRA ISSUES THAT HAVE GITHUB PULL REQUESTS")
        jira_github_unresolved_not_open = JiraGitHubCombinedReport(jira_unresolved, github_unresolved_not_open_jira,
                                                                   "\nIN PROGRESS OR REOPENED JIRA ISSUES THAT HAVE GITHUB PULL REQUESTS")
        jira_github_unresolved_voted = JiraGitHubCombinedReport(jira_unresolved, github_unresolved_jira_voted,
                                                                "\nGITHUB PULL REQUESTS WITH VOTES FOR UNRESOLVED JIRAS", True)
        # jira_github_unresolved = JiraGitHubCombinedReport(jira_unresolved, github_unresolved_jira,
        #                                                   "\nUnresolved JIRA issues with GitHub pull requests")

        jira_open_no_pull = JiraReport(jira_open.view(github_open_jira.jira_ids()),
                                       "\nOPEN JIRA ISSUES THAT DON'T HAVE GITHUB PULL REQUESTS")

        # build complete report
        self.report.jira_reports.append(jira_in_progress)
        self.report.jira_reports.append(jira_open_no_pull)

        self.report.github_reports.append(github_bad_jira)
        self.report.github_reports.append(github_without_jira)

        self.report.jira_github_combined_reports.append(jira_github_open)
        self.report.jira_github_combined_reports.append(jira_github_unresolved_voted)
        self.report.jira_github_combined_reports.append(jira_github_unresolved_not_open)
        # self.report.jira_github_combined_reports.append(jira_github_unresolved)
