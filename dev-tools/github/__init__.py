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

def gittime(obj):
	if (obj == None):
		return None
	return datetime.strptime(obj[0:19], "%Y-%m-%dT%H:%M:%S")

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

	def merged(self):
		return self.data["merged_at"] != None
		
	def raw(self):
		return self.data

	def created_at(self):
		return gittime(self.data["created_at"])

	def updated_at(self):
		return gittime(self.data["updated_at"])

	def merged_at(self):
		return gittime(self.data["merged_at"])	
	
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
