## IMPORTANT NOTE!!!
Storm has Moved to Apache. The official Storm git repository is now hosted by Apache, and is mirrored on github here:

[https://github.com/apache/incubator-storm](https://github.com/apache/incubator-storm)


### Contributing
Source code contributions can be submitted either by [sumitting a pull request](https://github.com/apache/incubator-storm/pulls) or by creating an issue in [JIRA](https://issues.apache.org/jira/browse/STORM) and attaching patches.

### Migrating Git Repos from nathanmarz/storm to apache/incubator-storm
If you have an existing fork/clone of nathanmarz/storm, you can migrate to apache/incubator-storm by doing the following:

1. Create a new fork of [apache/incubator-storm]()
2. Point your existing clone to the new fork:


		git remote remove origin
		git remote add origin git@github.com:username/incubator-storm.git



### Issue Tracking
The official issue tracker for Storm is Apache JIRA:

[https://issues.apache.org/jira/browse/STORM](https://issues.apache.org/jira/browse/STORM)



### User Mailing List
Storm users should send messages and subscribe to [user@storm.incubator.apache.org](mailto:user@storm.incubator.apache.org).

You can subscribe to this list by sending an email to [user-subscribe@storm.incubator.apache.org](mailto:user-subscribe@storm.incubator.apache.org). Likewise, you can cancel a subscription by sending an email to [user-unsubscribe@storm.incubator.apache.org](mailto:user-unsubscribe@storm.incubator.apache.org).

You can view the archives of the mailing list [here](http://mail-archives.apache.org/mod_mbox/incubator-storm-user/).

### Developer Mailing List
Storm developers should send messages and subscribe to [dev@storm.incubator.apache.org](mailto:dev@storm.incubator.apache.org).

You can subscribe to this list by sending an email to [dev-subscribe@storm.incubator.apache.org](mailto:dev-subscribe@storm.incubator.apache.org). Likewise, you can cancel a subscription by sending an email to [dev-unsubscribe@storm.incubator.apache.org](mailto:dev-unsubscribe@storm.incubator.apache.org).

You can view the archives of the mailing list [here](http://mail-archives.apache.org/mod_mbox/incubator-storm-dev/).

### Which list should I send/subscribe to?
If you are using a pre-built binary distribution of Storm, then chances are you should send questions, comments, storm-related announcements, etc. to [user@storm.apache.incubator.org](user@storm.apache.incubator.org). 

If you are building storm from source, developing new features, or otherwise hacking storm source code, then [dev@storm.incubator.apache.org](dev@storm.incubator.apache.org) is more appropriate. 

### What will happen with storm-user@googlegroups.com?
All existing messages will remain archived there, and can be accessed/searched [here](https://groups.google.com/forum/#!forum/storm-user).

New messages sent to storm-user@googlegroups.com will either be rejected/bounced or replied to with a message to direct the email to the appropriate Apache-hosted group.
