---
layout: post
title: Storm 0.9.4 released
author: P. Taylor Goetz
---

The Apache Storm community is pleased to announce that version 0.9.4 has been released and is available from [the downloads page](/downloads.html).

This is a maintenance release that includes a number of important bug fixes that improve Storm's stability and fault tolerance. We encourage users of previous versions to upgrade to this latest release.


Thanks
------
Special thanks are due to all those who have contributed to Apache Storm -- whether through direct code contributions, documentation, bug reports, or helping other users on the mailing lists. Your efforts are much appreciated.


Full Changelog
---------

 * STORM-559: ZkHosts in README should use 2181 as port.
 * STORM-682: supervisor should handle worker state corruption gracefully.
 * STORM-693: when kafka bolt fails to write tuple, it should report error instead of silently acking.
 * STORM-329: fix cascading Storm failure by improving reconnection strategy and buffering messages
 * STORM-130: Supervisor getting killed due to java.io.FileNotFoundException: File '../stormconf.ser' does not exist.