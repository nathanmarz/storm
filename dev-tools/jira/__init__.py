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

import re
import urllib
import urllib2
from datetime import datetime
try:
	import json
except ImportError:
	import simplejson as json

def mstr(obj):
	if (obj == None):
		return ""
	return unicode(obj)

def jiratime(obj):
	if (obj == None):
		return None
	return datetime.strptime(obj[0:19], "%Y-%m-%dT%H:%M:%S")

githubUser = re.compile("Git[Hh]ub user ([\w-]+)")
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

	def raw(self):
		return self.data

	def hasVote(self):
		return self.vote != None

	def getVote(self):
		return self.vote

	def getCreated(self):
		return jiratime(self.data['created'])

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

	def getCreated(self):
		return jiratime(self.fields['created'])

	def getUpdated(self):
		return jiratime(self.fields['updated'])
	
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
	
	def query(self, query):
		jiras = {}
		at=0
		end=1
		count=100
		while (at < end):
			params = urllib.urlencode({'jql': query, 'startAt':at, 'maxResults':count})
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
	
	def openJiras(self, project):
		return self.query("project = "+project+" AND resolution = Unresolved");

