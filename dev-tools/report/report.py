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

from datetime import datetime
from github import mstr
from jira import Jira
from formatter import Formatter, encode


def daydiff(a, b):
    return (a - b).days


class Report:
    now = datetime.utcnow()
    def __init__(self, header=''):
        self.header = header

    # if padding starts with - it puts padding before contents, otherwise after
    @staticmethod
    def _build_tuple(contents, padding=''):
        if padding is not '':
            out = []
            for i in range(len(contents)):
                out += [padding[1:] + str(contents[i])] if padding[0] is '-' else [str(contents[i]) + padding]
            return tuple(out)
        return contents

    # calls the native print function with the following format. Text1,Text2,... has the correct spacing
    # print ("%s%s%s" % ("Text1, Text2, Text3))
    def print_(self, formatter, row_tuple):
        print (formatter.format % formatter.row_str_format(row_tuple))

class JiraReport(Report):
    def __init__(self, issues, header=''):
        Report.__init__(self, header)
        self.issues = issues

    def view(self, excluded):
        issues_view = dict(self.issues)
        for key in excluded:
            issues_view.pop(key, None)
        return issues_view

    def keys_view(self, excluded):
        return self.view(excluded).keys().sort(Jira.storm_jira_cmp, reverse=True)

    def values_view(self, excluded=None):
        temp_dic = dict(self.issues) if excluded is None else self.view(excluded)
        values = temp_dic.values()
        values.sort(Jira.storm_jira_cmp, reverse=True)
        return values

    @staticmethod
    def _row_tuple(jira):
        return (jira.get_id(), jira.get_trimmed_summary(), daydiff(Report.now, jira.get_created()),
                daydiff(Report.now, jira.get_updated()))

    def _min_width_tuple(self):
        return -1, 43, -1, -1

    def print_report(self):
        print "%s (Count = %s) " % (self.header, len(self.issues))
        jiras = self.values_view()
        fields_tuple = ('Jira Id', 'Summary', 'Created', 'Last Updated (Days)')
        row_tuple = self._row_tuple(jiras[0])

        formatter = Formatter(fields_tuple, row_tuple, self._min_width_tuple())

        self.print_(formatter, fields_tuple)

        for jira in jiras:
            row_tuple = self._row_tuple(jira)
            self.print_(formatter, row_tuple)

    @staticmethod
    def build_jira_url(jira_id):
        BASE_URL = "https://issues.apache.org/jira/browse/"
        return BASE_URL + jira_id


class GitHubReport(Report):
    def __init__(self, pull_requests=None, header=''):
        Report.__init__(self, header)

        if pull_requests is None:
            self.pull_requests = []
            self.type = ''
        else:
            self.pull_requests = pull_requests
            self.type = type

    def _row_tuple(self, pull):
        return self._build_tuple(
            (pull.html_url(), pull.trimmed_title(), daydiff(Report.now, pull.created_at()),
             daydiff(Report.now, pull.updated_at()), pull.user()), '')

    def _min_width_tuple(self):
        return -1, 43, -1, -1, -1

    def print_report(self):
        print "%s (Count = %s) " % (self.header, len(self.pull_requests))

        fields_tuple = self._build_tuple(('URL', 'Title', 'Created', 'Last Updated (Days)', 'User'), '')
        if len(self.pull_requests) > 0:
            row_tuple = self._row_tuple(self.pull_requests[0])

            formatter = Formatter(fields_tuple, row_tuple, self._min_width_tuple())

            self.print_(formatter, fields_tuple)
            for pull in self.pull_requests:
                row_tuple = self._row_tuple(pull)
                self.print_(formatter, row_tuple)

    def jira_ids(self):
        """
        :return: sorted list of JIRA ids present in Git pull requests
        """
        jira_ids = list()
        for pull in self.pull_requests:
            jira_ids.append(pull.jira_id())
        return sorted(jira_ids)

class JiraGitHubCombinedReport(Report):
    def __init__(self, jira_report, github_report, header='', print_comments=False):
        Report.__init__(self, header)
        self.jira_report = jira_report
        self.github_report = github_report
        self.print_comments = print_comments

    def _jira_comments(self, jira_id):
        return None if jira_id is None else self.jira_report.issues[jira_id].get_comments()

    def _idx_1st_comment_with_vote(self):
        g = 0
        for pull in self.github_report.pull_requests:
            c = 0
            for comment in self._jira_comments(pull.jira_id()):
                if comment.has_vote():
                    return(g,) + (c,)
                c += 1
            g += 1

    def _pull_request(self, pull_idx):
        pull = self.github_report.pull_requests[pull_idx]
        return pull

    def _jira_id(self, pull_idx):
        pull = self._pull_request(pull_idx)
        return encode(pull.jira_id())

    def _jira_issue(self, jira_id):
        return self.jira_report.issues[jira_id]

    def _row_tuple(self, pull_idx):
        pull = self._pull_request(pull_idx)
        jira_id = self._jira_id(pull_idx)
        jira_issue = self._jira_issue(jira_id)

        return (jira_id, mstr(pull), jira_issue.get_trimmed_summary(),
                daydiff(Report.now, jira_issue.get_created()),
                daydiff(Report.now, pull.created_at()),
                daydiff(Report.now, jira_issue.get_updated()),
                daydiff(Report.now, pull.updated_at()),
                jira_issue.get_status(), pull.user())

    def _row_tuple_1(self, pull_idx, comment_idx):
        row_tuple_1 = None
        jira_id = self._jira_id(pull_idx)
        jira_comments = self._jira_comments(jira_id)
        comment = jira_comments[comment_idx]
        if comment.has_vote():
            row_tuple_1 = (comment.get_vote(), comment.get_author(), comment.get_pull(),
                           daydiff(Report.now, comment.get_created()))

        return row_tuple_1

    # variables and method names ending with _1 correspond to the comments part
    def print_report(self, print_comments=False):
        print "%s (Count = %s) " % (self.header, len(self.github_report.pull_requests))

        fields_tuple = ('JIRA ID', 'Pull Request', 'Jira Summary', 'JIRA Age',
                        'Pull Age', 'JIRA Update Age', 'Pull Update Age (Days)',
                        'JIRA Status', 'GitHub user')
        row_tuple = self._row_tuple(0)
        formatter = Formatter(fields_tuple, row_tuple)
        self.print_(formatter, fields_tuple)

        row_tuple_1 = ()
        formatter_1 = Formatter()

        if print_comments or self.print_comments:
            fields_tuple_1 = self._build_tuple(('Comment Vote', 'Comment Author', 'Pull URL', 'Comment Age'), '-\t\t')
            row_tuple_1 = self._build_tuple(self._row_tuple_1(*self._idx_1st_comment_with_vote()), '-\t\t')
            formatter_1 = Formatter(fields_tuple_1, row_tuple_1)
            self.print_(formatter_1, fields_tuple_1)
            print ''

        for p in range(0, len(self.github_report.pull_requests)):
            row_tuple = self._row_tuple(p)
            self.print_(formatter, row_tuple)

            if print_comments or self.print_comments:
                has_vote = False
                comments = self._jira_comments(self._jira_id(p))
                for c in range(len(comments)):     # Check cleaner way
                    comment = comments[c]
                    if comment.has_vote():
                        row_tuple_1 = self._build_tuple(self._row_tuple_1(p, c), '-\t\t')
                        if row_tuple_1 is not None:
                            self.print_(formatter_1, row_tuple_1)
                            has_vote = True
                if has_vote:
                    print ''


class CompleteReport(Report):
    def __init__(self, header=''):
        Report.__init__(self, header)
        self.jira_reports = []
        self.github_reports = []
        self.jira_github_combined_reports = []

    def print_all(self):
        if self.header is not '':
            print self.header

        self._print_github_reports()
        self._print_jira_github_combined_reports()
        self._print_jira_reports()

    def _print_jira_reports(self):
        for jira in self.jira_reports:
            jira.print_report()

    def _print_github_reports(self):
        for github in self.github_reports:
            github.print_report()

    def _print_jira_github_combined_reports(self):
        for jira_github_combined in self.jira_github_combined_reports:
            jira_github_combined.print_report()
