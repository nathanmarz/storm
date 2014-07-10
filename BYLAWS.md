# [DRAFT] Apache Storm Project Bylaws

NOTE: This document is a work in progress and has not yet been formally adopted by the Storm (P)PMC. It is expected that a final version of this DRAFT be adopted once Storm has graduated to an Apache TLP. Until that time any guidelines listed below are subject to change.

## Roles and Responsibilities

Apache projects define a set of roles with associated rights and responsibilities. These roles govern what tasks an individual may perform within the project. The roles are defined in the following sections:

### Users:

The most important participants in the project are people who use our software. The majority of our developers start out as users and guide their development efforts from the user's perspective.

Users contribute to the Apache projects by providing feedback to developers in the form of bug reports and feature suggestions. As well, users participate in the Apache community by helping other users on mailing lists and user support forums.

### Contributors:

Contributors are all of the volunteers who are contributing time, code, documentation, or resources to the Storm Project. A contributor that makes sustained, welcome contributions to the project may be invited to become a Committer, though the exact timing of such invitations depends on many factors.

### Committers:

The project's Committers are responsible for the project's technical management. Committers have access to all project source repositories. Committers may cast binding votes on any technical discussion regarding storm.

Committer access is by invitation only and must be approved by lazy consensus of the active PMC members. A Committer is considered emeritus by their own declaration or by not contributing in any form to the project for over six months. An emeritus Committer may request reinstatement of commit access from the PMC. Such reinstatement is subject to lazy consensus approval of active PMC members.

All Apache Committers are required to have a signed Contributor License Agreement (CLA) on file with the Apache Software Foundation. There is a [Committers' FAQ](https://www.apache.org/dev/committers.html) which provides more details on the requirements for Committers.

A Committer who makes a sustained contribution to the project may be invited to become a member of the PMC. The form of contribution is not limited to code. It can also include code review, helping out users on the mailing lists, documentation, testing, etc.

### Project Management Committee(PMC):

The PMC is responsible to the board and the ASF for the management and oversight of the Apache Storm codebase. The responsibilities of the PMC include:

 * Deciding what is distributed as products of the Apache Storm project. In particular all releases must be approved by the PMC.
 * Maintaining the project's shared resources, including the codebase repository, mailing lists, websites.
 * Speaking on behalf of the project.
 * Resolving license disputes regarding products of the project.
 * Nominating new PMC members and Committers.
 * Maintaining these bylaws and other guidelines of the project.

Membership of the PMC is by invitation only and must be approved by a consensus approval of active PMC members. A PMC member is considered "emeritus" by their own declaration or by not contributing in any form to the project for over six months. An emeritus member may request reinstatement to the PMC. Such reinstatement is subject to consensus approval of the active PMC members.

The chair of the PMC is appointed by the ASF board. The chair is an office holder of the Apache Software Foundation (Vice President, Apache Storm) and has primary responsibility to the board for the management of the projects within the scope of the Storm PMC. The chair reports to the board quarterly on developments within the Storm project.

The chair of the PMC is rotated annually. When the chair is rotated or if the current chair of the PMC resigns, the PMC votes to recommend a new chair using Single Transferable Vote (STV) voting. See http://wiki.apache.org/general/BoardVoting for specifics. The decision must be ratified by the Apache board.

## Voting

Decisions regarding the project are made by votes on the primary project development mailing list (dev@storm.incubator.apache.org). Where necessary, PMC voting may take place on the private Storm PMC mailing list. Votes are clearly indicated by subject line starting with [VOTE]. Votes may contain multiple items for approval and these should be clearly separated. Voting is carried out by replying to the vote mail. Voting may take four flavors:
	
| Vote | Meaning |
|------|---------|
| +1 | 'Yes,' 'Agree,' or 'the action should be performed.' |
| +0 | Neutral about the proposed action. |
| -0 | Mildly negative, but not enough so to want to block it. |
| -1 |This is a negative vote. On issues where consensus is required, this vote counts as a veto. All vetoes must contain an explanation of why the veto is appropriate. Vetoes with no explanation are void. It may also be appropriate for a -1 vote to include an alternative course of action. |

All participants in the Storm project are encouraged to show their agreement with or against a particular action by voting. For technical decisions, only the votes of active Committers are binding. Non-binding votes are still useful for those with binding votes to understand the perception of an action in the wider Storm community. For PMC decisions, only the votes of active PMC members are binding.

Voting can also be applied to changes already made to the Storm codebase. These typically take the form of a veto (-1) in reply to the commit message sent when the commit is made. Note that this should be a rare occurrence. All efforts should be made to discuss issues when they are still patches before the code is committed.

Only active (i.e. non-emeritus) Committers and PMC members have binding votes.

## Approvals

These are the types of approvals that can be sought. Different actions require different types of approvals

| Approval Type | Criteria |
|---------------|----------|
| Consensus Approval | Consensus approval requires 3 binding +1 votes and no binding vetoes. |
| Lazy Consensus | Lazy consensus requires no -1 votes ('silence gives assent'). |
| Lazy Majority | A lazy majority vote requires 3 binding +1 votes and more binding +1 votes than -1 votes. |
| Lazy 2/3 Majority | Lazy 2/3 majority votes requires at least 3 votes and twice as many +1 votes as -1 votes. |

### Vetoes

A valid, binding veto cannot be overruled. If a veto is cast, it must be accompanied by a valid reason explaining the reasons for the veto. The validity of a veto, if challenged, can be confirmed by anyone who has a binding vote. This does not necessarily signify agreement with the veto - merely that the veto is valid.

If you disagree with a valid veto, you must lobby the person casting the veto to withdraw their veto. If a veto is not withdrawn, any action that has been vetoed must be reversed in a timely manner.

## Actions

This section describes the various actions which are undertaken within the project, the corresponding approval required for that action and those who have binding votes over the action.

| Actions | Description | Approval | Binding Votes | Minimum Length | Mailing List |
|---------|-------------|----------|---------------|----------------|--------------|
| Code Change | A change made to a source code of the project and committed by a Committer. | One +1 from a Committer other than the one who authored the patch, and no -1s. Code changes to a release require a re-vote on that release, but non-code changes do not require a re-vote. | Active Committers | 2 days from initial patch |JIRA or Github pull ( with notification sent to dev@storm.incubator.apache.org) |
| Non-Code Change | A change made to a repository of the project and committed by a Committer. This includes documentation, website content, etc., but not source code, unless only comments are being modified. | Lazy Consensus | Active Committers | At the discression of the Committer |JIRA or Github pull (with notification sent to dev@storm.incubator.apache.org) |
| Product Release | A vote is required to accept a proposed release as an official release of the project. Any Committer may call for a release vote at any point in time. | Lazy Majority | Active PMC members | 7 days | dev@storm.incubator.apache.org |
| Adoption of New Codebase | When the codebase for an existing, released product is to be replaced with an alternative codebase. If such a vote fails to gain approval, the existing code base will continue. This also covers the creation of new sub-projects and submodules within the project. | Lazy 2/3 majority | Active PMC members | 7 days | dev@storm.incubator.apache.org |
| New Committer | When a new Committer is proposed for the project. | Lazy consensus | Active PMC members | 7 days | private@storm.incubator.apache.org |
| New PMC Member | When a Committer is proposed for the PMC. | Consensus Approval | Active PMC members | 7 days | private@storm.incubator.apache.org |
| Emeritus PMC Member re-instatement | When an emeritus PMC member requests to be re-instated as an active PMC member. | Consensus Approval | Active PMC members | 7 days | private@storm.incubator.apache.org |
| Emeritus Committer re-instatement | When an emeritus Committer requests to be re-instated as an active Committer. | Consensus Approval | Active PMC members | 7 days | private@storm.incubator.apache.org |
| Committer Removal | When removal of commit privileges is sought. Note: Such actions will also be referred to the ASF board by the PMC chair. | Consensus Approval | Active PMC members (excluding the Committer in question if a member of the PMC). | 7 Days | private@storm.incubator.apache.org |
| PMC Member Removal | When removal of a PMC member is sought. Note: Such actions will also be referred to the ASF board by the PMC chair. | Consensus Approval | Active PMC members (excluding the member in question). | 7 Days | private@storm.incubator.apache.org |
| Modifying Bylaws | Modifying this document. | Lazy 2/3 majority | Active PMC members | 7 Days | dev@storm.incubator.apache.org |
