#!/usr/bin/python
#	 Licensed under the Apache License, Version 2.0 (the "License");
#	 you may not use this file except in compliance with the License.
#	 You may obtain a copy of the License at
#
#			 http://www.apache.org/licenses/LICENSE-2.0
#
#	 Unless required by applicable law or agreed to in writing, software
#	 distributed under the License is distributed on an "AS IS" BASIS,
#	 WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#	 See the License for the specific language governing permissions and
#	 limitations under the License.

from github import GitHub
from optparse import OptionParser

def main():
	parser = OptionParser(usage="usage: %prog [options] [pull number]")
	parser.add_option("-g", "--github-user", dest="gituser",
			type="string", help="github user, if not supplied no auth is used", metavar="USER")
	
	(options, args) = parser.parse_args()
	github = GitHub(options)

        for pullNumber in args:
		pull = github.pull("apache", "storm", pullNumber)
		print "git pull --no-ff "+pull.from_repo()+" "+pull.from_branch()

if __name__ == "__main__":
	main()

