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

from jira import JiraRepo
from github import GitHub, mstr
import re
from optparse import OptionParser
from datetime import datetime

def daydiff(a, b):
	return (a - b).days

def main():
	parser = OptionParser(usage="usage: %prog [options]")
	parser.add_option("-g", "--github-user", dest="gituser",
			type="string", help="github user, if not supplied no auth is used", metavar="USER")
	
	(options, args) = parser.parse_args()
	
	jrepo = JiraRepo("https://issues.apache.org/jira/rest/api/2")
	github = GitHub(options)
	
	openPullRequests = github.openPulls("apache","incubator-storm")
	stormJiraNumber = re.compile("STORM-[0-9]+")
	openJiras = jrepo.openJiras("STORM")
	
	jira2Pulls = {}
	pullWithoutJira = []
	pullWithBadJira = []
	
	for pull in openPullRequests:
		found = stormJiraNumber.search(pull.title())
		if found:
			jiraNum = found.group(0)
			if not (jiraNum in openJiras):
				pullWithBadJira.append(pull)
			else:
				if jira2Pulls.get(jiraNum) == None:
					jira2Pulls[jiraNum] = []
				jira2Pulls[jiraNum].append(pull)
		else:
			pullWithoutJira.append(pull);
	
	now = datetime.utcnow()
	print "Pull requests that need a JIRA:"
	print "Pull URL\tPull Title\tPull Age\tPull Update Age"
	for pull in pullWithoutJira:
		print ("%s\t%s\t%s\t%s"%(pull.html_url(), pull.title(), daydiff(now, pull.created_at()), daydiff(now, pull.updated_at()))).encode("UTF-8")
	
	print "\nPull with bad or closed JIRA:"
	print "Pull URL\tPull Title\tPull Age\tPull Update Age"
	for pull in pullWithBadJira:
		print ("%s\t%s\t%s\t%s"%(pull.html_url(), pull.title(), daydiff(now, pull.created_at()), daydiff(now, pull.updated_at()))).encode("UTF-8")
	
	print "\nOpen JIRA to Pull Requests and Possible Votes, vote detection is very approximate:"
	print "JIRA\tPull Requests\tJira Summary\tJIRA Age\tPull Age\tJIRA Update Age\tPull Update Age"
	print "\tComment Vote\tComment Author\tPull URL\tComment Age"
	for key, value in jira2Pulls.items():
		print ("%s\t%s\t%s\t%s\t%s\t%s\t%s"%(key, mstr(value), openJiras[key].getSummary(),
			 daydiff(now, openJiras[key].getCreated()), daydiff(now, value[0].created_at()),
			 daydiff(now, openJiras[key].getUpdated()), daydiff(now, value[0].updated_at()))).encode("UTF-8")
		for comment in openJiras[key].getComments():
			#print comment.raw()
			if comment.hasVote():
				print (("\t%s\t%s\t%s\t%s")%(comment.getVote(), comment.getAuthor(), comment.getPull(), daydiff(now, comment.getCreated()))).encode("UTF-8")

if __name__ == "__main__":
	main()

