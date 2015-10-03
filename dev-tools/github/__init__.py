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

import getpass
import base64
import urllib2
from datetime import datetime
import re

try:
    import json
except ImportError:
    import simplejson as json


def mstr(obj):
    if obj is None:
        return ""
    return unicode(obj)


def git_time(obj):
    if obj is None:
        return None
    return datetime.strptime(obj[0:19], "%Y-%m-%dT%H:%M:%S")


class GitPullRequest:
    """Pull Request from Git"""

    storm_jira_number = re.compile("STORM-[0-9]+", re.I)

    def __init__(self, data, parent):
        self.data = data
        self.parent = parent

    def html_url(self):
        return self.data["html_url"]

    def title(self):
        return self.data["title"]

    def trimmed_title(self):
        limit = 40
        title = self.data["title"]
        return title if len(title) < limit else title[0:limit] + "..."

    def number(self):
        return self.data["number"]

        # TODO def review_comments

    def user(self):
        return mstr(self.data["user"]["login"])

    def from_branch(self):
        return mstr(self.data["head"]["ref"])

    def from_repo(self):
        return mstr(self.data["head"]["repo"]["clone_url"])

    def merged(self):
        return self.data["merged_at"] is not None

    def raw(self):
        return self.data

    def created_at(self):
        return git_time(self.data["created_at"])

    def updated_at(self):
        return git_time(self.data["updated_at"])

    def merged_at(self):
        return git_time(self.data["merged_at"])

    def has_jira_id(self):
        return GitPullRequest.storm_jira_number.search(self.title())

    def jira_id(self):
        return GitPullRequest.storm_jira_number.search(self.title()).group(0).upper()

    def __str__(self):
        return self.html_url()

    def __repr__(self):
        return self.html_url()


class GitHub:
    """Github API"""

    def __init__(self, options):
        self.headers = {}
        if options.gituser:
            gitpassword = getpass.getpass("github.com user " + options.gituser + ":")
            authstr = base64.encodestring('%s:%s' % (options.gituser, gitpassword)).replace('\n', '')
            self.headers["Authorization"] = "Basic " + authstr

    def pulls(self, user, repo, type="all"):
        page = 1
        ret = []
        while True:
            url = "https://api.github.com/repos/" + user + "/" + repo + "/pulls?state=" + type + "&page=" + str(page)

            req = urllib2.Request(url, None, self.headers)
            result = urllib2.urlopen(req)
            contents = result.read()
            if result.getcode() != 200:
                raise Exception(result.getcode() + " != 200 " + contents)
            got = json.loads(contents)
            for part in got:
                ret.append(GitPullRequest(part, self))
            if len(got) == 0:
                return ret
            page = page + 1

    def open_pulls(self, user, repo):
        return self.pulls(user, repo, "open")

    def pull(self, user, repo, number):
        url = "https://api.github.com/repos/" + user + "/" + repo + "/pulls/" + number
        req = urllib2.Request(url, None, self.headers)
        result = urllib2.urlopen(req)
        contents = result.read()
        if result.getcode() != 200:
            raise Exception(result.getcode() + " != 200 " + contents)
        got = json.loads(contents)
        return GitPullRequest(got, self)
