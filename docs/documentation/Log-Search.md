Log Search
==========================

This feature is aimed for improving the debugging of Storm. Log Search supports searching in a certain log file or in a topology's all log files:

Using the Storm UI
-------------
String search in a log file: In the log page for a worker, user can search a certain string, e.g., "Exception", in for a certain worker log. This search can happen for both normal text log or rolled ziplog files. In the results, the offset and matched lines will be displayed.

![Search in a log](images/search-for-a-single-worker-log.png "Seach in a log")

Search in a topology: User can also search a string for a certain topology by clicking the icon of magnifying lens at the top right corner of the UI page. This means the UI will try to search on all the supervisor nodes in a distributed way to find the matched string in all logs for this topology. The search can happen for both normal text log or rolled zip log files by checking/unchecking the "Search archived logs:" box. Then the matched results can be shown on the UI with url links, directing user to the certain logs on each supervisor node. This powerful feature is very helpful for users to find certain problematic supervisor nodes running this topology.

![Seach in a topology](images/search-a-topology.png "Search in a topology")
