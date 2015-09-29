---
layout: post
title: Storm 0.9.5 released
author: P. Taylor Goetz
---

The Apache Storm community is pleased to announce that version 0.9.5 has been released and is available from [the downloads page](/downloads.html).

This is a maintenance release that includes a number of important bug fixes that improve Storm's stability and fault tolerance. We encourage users of previous versions to upgrade to this latest release.


Thanks
------
Special thanks are due to all those who have contributed to Apache Storm -- whether through direct code contributions, documentation, bug reports, or helping other users on the mailing lists. Your efforts are much appreciated.


Full Changelog
---------

 * STORM-790: Log "task is null" instead of letting worker die when task is null in transfer-fn
 * STORM-796: Add support for "error" command in ShellSpout
 * STORM-745: fix storm.cmd to evaluate 'shift' correctly with 'storm jar'
 * STORM-130: Supervisor getting killed due to java.io.FileNotFoundException: File '../stormconf.ser' does not exist.
