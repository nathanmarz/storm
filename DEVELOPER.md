# Developer documentation

This document summarizes information relevant to Storm committers and contributors.  It includes information about
the development processes and policies as well as the tools we use to facilitate those.

---

Table of Contents

* <a href="#welcome">Welcome!</a>
* <a href="#workflow-and-policies">Workflows</a>
    * <a href="#report-bug">Report a bug</a>
    * <a href="#request-feature">Request a new feature</a>
    * <a href="#contribute-code">Contribute code</a>
    * <a href="#contribute-documentation">Contribute documentation</a>
    * <a href="#pull-requests">Pull requests</a>
        * <a href="#create-pull-request">Create a pull request</a>
        * <a href="#approve-pull-request">Approve a pull request</a>
        * <a href="#merge-pull-request">Merge a pull request or patch</a>
    * <a href="#building">Build the code and run the tests</a>
    * <a href="#packaging">Create a Storm distribution (packaging)</a>
* <a href="#best-practices">Best practices</a>
    * <a href="#best-practices-testing">Testing</a>
* <a href="#tools">Tools</a>
    * <a href="#code-repositories">Source code repositories (git)</a>
    * <a href="#issue-tracking">Issue tracking (JIRA)</a>
* <a href="#questions">Questions?</a>

---

<a name="welcome"></a>

# Welcome!

If you are reading this document then you are interested in contributing to the Storm project -- many thanks for that!
All contributions are welcome: ideas, documentation, code, patches, bug reports, feature requests, etc.  And you do not
need to be a programmer to speak up.


# Workflows

This section explains how to perform common activities such as reporting a bug or merging a pull request.


<a name="report-bug"></a>

## Report a bug

To report a bug you should [open an issue](https://issues.apache.org/jira/browse/STORM) in our issue tracker that
summarizes the bug.  Set the form field "Issue type" to "Bug".  If you have not used the issue tracker before you will
need to register an account (free), log in, and then click on the blue "Create Issue" button in the top navigation bar.

In order to help us understand and fix the bug it would be great if you could provide us with:

1. The steps to reproduce the bug.  This includes information about e.g. the Storm version you were using.
2. The expected behavior.
3. The actual, incorrect behavior.

Feel free to search the issue tracker for existing issues (aka tickets) that already describe the problem;  if there is
such a ticket please add your information as a comment.

**If you want to provide a patch along with your bug report:**
That is great!  In this case please send us a pull request as described in section _Create a pull request_ below.
You can also opt to attach a patch file to the issue ticket, but we prefer pull requests because they are easier to work
with.


<a name="request-feature"></a>

## Request a new feature

To request a new feature you should [open an issue](https://issues.apache.org/jira/browse/STORM) in our issue tracker
and summarize the desired functionality.  Set the form field "Issue type" to "New feature".  If you have not used the
issue tracker before you will need to register an account (free), log in, and then click on the blue "Create Issue"
button in the top navigation bar.

You can also opt to send a message to the [Storm Users mailing list](http://storm.incubator.apache.org/community.html).


<a name="contribute-code"></a>

## Contribute code

Before you set out to contribute code we recommend that you familiarize yourself with the Storm codebase, notably by
reading through the
[Implementation documentation](http://storm.incubator.apache.org/documentation/Implementation-docs.html).

_If you are interested in contributing code to Storm but do not know where to begin:_
In this case you should
[browse our issue tracker for open issues and tasks](https://issues.apache.org/jira/browse/STORM/?selectedTab=com.atlassian.jira.jira-projects-plugin:issues-panel).
You may want to start with beginner-friendly, easier issues
([newbie issues](https://issues.apache.org/jira/browse/STORM-58?jql=project%20%3D%20STORM%20AND%20labels%20%3D%20newbie%20AND%20status%20%3D%20Open)
and
[trivial issues](https://issues.apache.org/jira/secure/IssueNavigator.jspa?reset=true&jqlQuery=project+%3D+STORM+AND+resolution+%3D+Unresolved+AND+priority+%3D+Trivial+ORDER+BY+key+DESC&mode=hide))
because they require learning about only an isolated portion of the codebase and are a relatively small amount of work.

Please use idiomatic Clojure style, as explained in [this Clojure style guide][clj-SG]. Another useful reference is
the [Clojure Library Coding Standards][clj-LCS]. Perhaps the most important is consistenly writing a clear docstring
for functions, explaining the return value and arguments. As of this writing, the Storm codebase would benefit from
various style improvements.

[clj-SG]: https://github.com/bbatsov/clojure-style-guide
[clj-LCS]: http://dev.clojure.org/display/community/Library+Coding+Standards

Contributions to the Storm codebase should be sent as GitHub pull requests.  See section _Create a pull request_ below
for details.  If there is any problem with the pull request we can iterate on it using the commenting features of
GitHub.

* For _small patches_, feel free to submit pull requests directly for those patches.
* For _larger code contributions_, please use the following process. The idea behind this process is to prevent any
  wasted work and catch design issues early on.

    1. [Open an issue](https://issues.apache.org/jira/browse/STORM) on our issue tracker if a similar issue does not
       exist already.  If a similar issue does exist, then you may consider participating in the work on the existing
       issue.
    2. Comment on the issue with your plan for implementing the issue.  Explain what pieces of the codebase you are
       going to touch and how everything is going to fit together.
    3. Storm committers will iterate with you on the design to make sure you are on the right track.
    4. Implement your issue, create a pull request (see below), and iterate from there.


<a name="contribute-documentation"></a>

## Contribute documentation

Documentation contributions are very welcome!  The best way to send contributions is as emails through the
[Storm Developers](http://storm.incubator.apache.org/community.html) mailing list.


<a name="pull-requests"></a>

## Pull requests


<a name="create-pull-request"></a>

### Create a pull request

Pull requests should be done against the read-only git repository at
[https://github.com/apache/incubator-storm](https://github.com/apache/incubator-storm).

Take a look at [Creating a pull request](https://help.github.com/articles/creating-a-pull-request).  In a nutshell you
need to:

1. [Fork](https://help.github.com/articles/fork-a-repo) the Storm GitHub repository at
   [https://github.com/apache/incubator-storm/](https://github.com/apache/incubator-storm/) to your personal GitHub
   account.  See [Fork a repo](https://help.github.com/articles/fork-a-repo) for detailed instructions.
2. Commit any changes to your fork.
3. Send a [pull request](https://help.github.com/articles/creating-a-pull-request) to the Storm GitHub repository
   that you forked in step 1.  If your pull request is related to an existing Storm JIRA ticket -- for instance, because
   you reported a bug report via JIRA earlier -- then prefix the title of your pull request with the corresponding JIRA
   ticket number (e.g. `STORM-123: ...`).

You may want to read [Syncing a fork](https://help.github.com/articles/syncing-a-fork) for instructions on how to keep
your fork up to date with the latest changes of the upstream (official) `incubator-storm` repository.


<a name="approve-pull-request"></a>

### Approve a pull request

_NOTE: The information in this section may need to be formalized via proper project bylaws._

Pull requests are approved with two +1s from committers and need to be up for at least 72 hours for all committers to
have a chance to comment.  In case it was a committer who sent the pull request than two _different_ committers must +1
the request.


<a name="merge-pull-request"></a>

### Merge a pull request or patch

_This section applies to committers only._

**Important: A pull request must first be properly approved before you are allowed to merge it.**

Committers that are integrating patches or pull requests should use the official Apache repository at
[https://git-wip-us.apache.org/repos/asf/incubator-storm.git](https://git-wip-us.apache.org/repos/asf/incubator-storm.git).

To pull in a merge request you should generally follow the command line instructions sent out by GitHub.

1. Go to your local copy of the [Apache git repo](https://git-wip-us.apache.org/repos/asf/incubator-storm.git), switch
   to the `master` branch, and make sure it is up to date.

        $ git checkout master
        $ git fetch origin
        $ git merge origin/master

2. Create a local branch for integrating and testing the pull request.  You may want to name the branch according to the
   Storm JIRA ticket associated with the pull request (example: `STORM-1234`).

        $ git checkout -b <local_test_branch>  # e.g. git checkout -b STORM-1234

3. Merge the pull request into your local test branch.

        $ git pull <remote_repo_url> <remote_branch>

4.  Assuming that the pull request merges without any conflicts:
    Update the top-level `CHANGELOG.md`, and add in the JIRA ticket number (example: `STORM-1234`) and ticket
    description to the change log.  Make sure that you place the JIRA ticket number in the commit comments where
    applicable.

5. Run any sanity tests that you think are needed.

6. Once you are confident that everything is ok, you can merge your local test branch into your local `master` branch,
   and push the changes back to the official Apache repo.

        # Pull request looks ok, change log was updated, etc.  We are ready for pushing.
        $ git checkout master
        $ git merge <local_test_branch>  # e.g. git merge STORM-1234

        # At this point our local master branch is ready, so now we will push the changes
        # to the official Apache repo.  Note: The read-only mirror on GitHub will be updated
        # automatically a short while after you have pushed to the Apache repo.
        $ git push origin master

7. The last step is updating the corresponding JIRA ticket.  [Go to JIRA](https://issues.apache.org/jira/browse/STORM)
   and resolve the ticket.  Be sure to set the `Fix Version/s` field to the version you pushed your changes to.
   It is usually good practice to thank the author of the pull request for their contribution if you have not done
   so already.


<a name="building"></a>

# Build the code and run the tests

The following commands must be run from the top-level directory.

    # Build the code and run the tests
    $ mvn clean install

    # Build the code but skip the tests
    $ mvn clean install -DskipTests=true


<a name="packaging"></a>

## Create a Storm distribution (packaging)

You can create a _distribution_ (like what you can download from Apache) as follows.  Note that the instructions below
do not use the Maven release plugin because creating an official release is the task of our release manager.

    # First, build the code.
    $ mvn clean install  # you may skip tests with `-DskipTests=true` to save time

    # Create the binary distribution.
    $ cd storm-dist/binary && mvn package

The last command will create Storm binaries at:

    storm-dist/binary/target/apache-storm-<version>.pom
    storm-dist/binary/target/apache-storm-<version>.tar.gz
    storm-dist/binary/target/apache-storm-<version>.zip

including corresponding `*.asc` digital signature files.

After running `mvn package` you may be asked to enter your GPG/PGP credentials (once for each binary file, in fact).
This happens because the packaging step will create `*.asc` digital signatures for all the binaries, and in the workflow
above _your_ GPG private key will be used to create those signatures.

You can verify whether the digital signatures match their corresponding files:

    # Example: Verify the signature of the `.tar.gz` binary.
    $ gpg --verify storm-dist/binary/target/apache-storm-<version>.tar.gz.asc


<a name="best-practices"></a>

# Best practices


<a name="best-practices-testing"></a>

## Testing

Tests should never rely on timing in order to pass.  In Storm can properly test functionality that depends on time by
simulating time, which means we do not have to worry about e.g. random delays failing our tests indeterministically.

If you are testing topologies that do not do full tuple acking, then you should be testing using the "tracked
topologies" utilities in `backtype.storm.testing.clj`.  For example,
[test-acking](storm-core/test/clj/backtype/storm/integration_test.clj) (around line 213) tests the acking system in
Storm using tracked topologies.  Here, the key is the `tracked-wait` function: it will only return when both that many
tuples have been emitted by the spouts _and_ the topology is idle (i.e. no tuples have been emitted nor will be emitted
without further input).  Note that you should not use tracked topologies for topologies that have tick tuples.


<a name="tools"></a>

# Tools


<a name="code-repositories"></a>

## Source code repositories (git)

The source code of Storm is managed via [git](http://git-scm.com/).  For a number of reasons there is more than one git
repository associated with Storm.

* **Committers only:**
  [https://git-wip-us.apache.org/repos/asf/incubator-storm.git](https://git-wip-us.apache.org/repos/asf/incubator-storm.git)
  is the official and authoritative git repository for Storm, managed under the umbrella of the Apache Software
  Foundation.  Only official Storm committers will interact with this repository.
  When you push the first time to this repository git will prompt you for your username and password.  Use your Apache
  user ID and password, i.e. the credentials you configured via [https://id.apache.org/](https://id.apache.org/) after
  you were [onboarded as a committer](http://www.apache.org/dev/new-committers-guide.html#account-creation).
* **Everybody else:**
  [https://github.com/apache/incubator-storm/](https://github.com/apache/incubator-storm/) is a read-only mirror of the
  official git repository.  If you are not a Storm committer (most people) this is the repository you should work
  against.  See _Development workflow_ above on how you can create a pull request, for instance.

An automated bot (called _[ASF GitHub Bot](https://issues.apache.org/jira/secure/ViewProfile.jspa?name=githubbot)_ in
[Storm JIRA](https://issues.apache.org/jira/browse/STORM)) runs periodically to merge changes in the
[official Apache repo](https://git-wip-us.apache.org/repos/asf/incubator-storm.git) to the read-only
[GitHub mirror repository](https://github.com/apache/incubator-storm/), and to merge comments in GitHub pull requests to
the [Storm JIRA](https://issues.apache.org/jira/browse/STORM).


<a name="issue-tracking"></a>

## Issue tracking (JIRA)

Issue tracking includes tasks such as reporting bugs, requesting and collaborating on new features, and administrative
activities for release management.  As an Apache software project we use JIRA as our issue tracking tool.

The Storm JIRA is available at:

* [https://issues.apache.org/jira/browse/STORM](https://issues.apache.org/jira/browse/STORM)


If you do not have a JIRA account yet, then you can create one via the link above (registration is free).


<a name="questions"></a>

# Questions?

If you have any questions after reading this document, then please reach out to us via the
[Storm Developers](http://storm.incubator.apache.org/community.html) mailing list.

And of course we also welcome any contributions to improve the information in this document!
<a name="workflow"></a>


