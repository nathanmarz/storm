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

import getpass
import base64
import re
import sys
from optparse import OptionParser
import httplib
import urllib
import urllib2
try:
	import json
except ImportError:
	import simplejson as json

def mstr(obj):
	if (obj == None):
		return ""
	return unicode(obj)

githubUser = re.compile("Git[Hh]ub user (\w+)")
githubPull = re.compile("https://github.com/[^/\s]+/[^/\s]+/pull/[0-9]+")
hasVote = re.compile("\s+([-+][01])\s*")
isDiff = re.compile("--- End diff --")

def searchGroup(reg, txt, group):
	m = reg.search(txt)
	if m == None:
		return None
	return m.group(group)

class JiraComment:
	"""A comment on a JIRA"""

	def __init__(self, data):
		self.data = data
		self.author = mstr(self.data['author']['name'])
		self.githubAuthor = None
		self.githubPull = None
		self.githubComment = (self.author == "githubbot")
		body = self.getBody()
		if isDiff.search(body) != None:
			self.vote = None
		else:
			self.vote = searchGroup(hasVote, body, 1)

		if self.githubComment:
			self.githubAuthor = searchGroup(githubUser, body, 1)
			self.githubPull = searchGroup(githubPull, body, 0)
			

	def getAuthor(self):
		if self.githubAuthor != None:
			return self.githubAuthor
		return self.author

	def getBody(self):
		return mstr(self.data['body'])

	def getPull(self):
		return self.githubPull

	def hasVote(self):
		return self.vote != None

	def getVote(self):
		return self.vote

class Jira:
	"""A single JIRA"""
	
	def __init__(self, data, parent):
		self.key = data['key']
		self.fields = data['fields']
		self.parent = parent
		self.notes = None
		self.comments = None
	
	def getId(self):
		return mstr(self.key)
	
	def getDescription(self):
		return mstr(self.fields['description'])
	
	def getReleaseNote(self):
		if (self.notes == None):
			field = self.parent.fieldIdMap['Release Note']
			if (self.fields.has_key(field)):
				self.notes=mstr(self.fields[field])
			else:
				self.notes=self.getDescription()
		return self.notes
	
	def getPriority(self):
		ret = ""
		pri = self.fields['priority']
		if(pri != None):
			ret = pri['name']
		return mstr(ret)
	
	def getAssigneeEmail(self):
		ret = ""
		mid = self.fields['assignee']
		if mid != None:
			ret = mid['emailAddress']
		return mstr(ret)

	
	def getAssignee(self):
		ret = ""
		mid = self.fields['assignee']
		if(mid != None):
			ret = mid['displayName']
		return mstr(ret)
	
	def getComponents(self):
		return " , ".join([ comp['name'] for comp in self.fields['components'] ])
	
	def getSummary(self):
		return self.fields['summary']
	
	def getType(self):
		ret = ""
		mid = self.fields['issuetype']
		if(mid != None):
			ret = mid['name']
		return mstr(ret)
	
	def getReporter(self):
		ret = ""
		mid = self.fields['reporter']
		if(mid != None):
			ret = mid['displayName']
		return mstr(ret)
	
	def getProject(self):
		ret = ""
		mid = self.fields['project']
		if(mid != None):
			ret = mid['key']
		return mstr(ret)
	
	def getComments(self):
		if self.comments == None:
			jiraId = self.getId()
			comments = []
			at=0
			end=1
			count=100
			while (at < end):
				params = urllib.urlencode({'startAt':at, 'maxResults':count})
				resp = urllib2.urlopen(self.parent.baseUrl+"/issue/"+jiraId+"/comment?"+params)
				data = json.loads(resp.read())
				if (data.has_key('errorMessages')):
					raise Exception(data['errorMessages'])
				at = data['startAt'] + data['maxResults']
				end = data['total']
				for item in data['comments']:
					j = JiraComment(item)
					comments.append(j)
			self.comments = comments
		return self.comments
	
	def raw(self):
		return self.fields

class JiraRepo:
	"""A Repository for JIRAs"""
	
	def __init__(self, baseUrl):
		self.baseUrl = baseUrl
		resp = urllib2.urlopen(baseUrl+"/field")
		data = json.loads(resp.read())
		
		self.fieldIdMap = {}
		for part in data:
			self.fieldIdMap[part['name']] = part['id']
	
	def get(self, id):
		resp = urllib2.urlopen(self.baseUrl+"/issue/"+id)
		data = json.loads(resp.read())
		if (data.has_key('errorMessages')):
			raise Exception(data['errorMessages'])
		j = Jira(data, self)
		return j
	
	def openJiras(self, project):
		jiras = {}
		at=0
		end=1
		count=100
		while (at < end):
			params = urllib.urlencode({'jql': "project = "+project+" AND resolution = Unresolved", 'startAt':at, 'maxResults':count})
			#print params
			resp = urllib2.urlopen(self.baseUrl+"/search?%s"%params)
			data = json.loads(resp.read())
			if (data.has_key('errorMessages')):
				raise Exception(data['errorMessages'])
			at = data['startAt'] + data['maxResults']
			end = data['total']
			for item in data['issues']:
					j = Jira(item, self)
					jiras[j.getId()] = j
		return jiras

class GitPullRequest:
	"""Pull Request from Git"""
	def __init__(self, data, parent):
		self.data = data
		self.parent = parent
	
	def html_url(self):
		return self.data["html_url"]
	
	def title(self):
		return self.data["title"]
	
	def number(self):
		return self.data["number"]
	
	#TODO def review_comments
	
	def user(self):
		return mstr(self.data["user"]["login"])
	
	def fromBranch(self):
		return mstr(self.data["head"]["ref"])
	
	def fromRepo(self):
		return mstr(self.data["head"]["repo"]["clone_url"])
	
	def __str__(self):
		return self.html_url()
	
	def __repr__(self):
		return self.html_url()

class GitHub:
	"""Github API"""
	def __init__(self, options):
		self.headers = {}
		if options.gituser:
			gitpassword = getpass.getpass("github.com user " + options.gituser+":")
			authstr = base64.encodestring('%s:%s' % (options.gituser, gitpassword)).replace('\n', '')
			self.headers["Authorization"] = "Basic "+authstr
	
	def pulls(self, user, repo, type="all"):
		page=1
		ret = []
		while True:
			url = "https://api.github.com/repos/"+user+"/"+repo+"/pulls?state="+type+"&page="+str(page)
		
			req = urllib2.Request(url,None,self.headers)
			result = urllib2.urlopen(req)
			contents = result.read()
			if result.getcode() != 200:
				raise Exception(result.getcode() + " != 200 "+ contents)
			got = json.loads(contents)
			for part in got:
				ret.append(GitPullRequest(part, self))
			if len(got) == 0:
				return ret
			page = page + 1
			
	def openPulls(self, user, repo):
		return self.pulls(user, repo, "open")

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
	
	print "Pull requests that need a JIRA:"
	for pull in pullWithoutJira:
		print ("%s\t%s"%(pull.html_url(), pull.title())).encode("UTF-8")
	
	print "\nPull with bad JIRA:"
	for pull in pullWithBadJira:
		print ("%s\t%s"%(pull.html_url(), pull.title())).encode("UTF-8")
	
	print "\nOpen JIRA to Pull Requests and Votes:"
	for key, value in jira2Pulls.items():
		print ("%s\t%s\t%s"%(key, mstr(value),openJiras[key].getSummary())).encode("UTF-8")
		for comment in openJiras[key].getComments():
			if comment.hasVote():
				print (("\t%s\t%s\t%s")%(comment.getVote(), comment.getAuthor(), comment.getPull())).encode("UTF-8")

if __name__ == "__main__":
	main()

