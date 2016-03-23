---
title: CGroup Enforcement
layout: documentation
documentation: true
---

# CGroups in Storm

CGroups are used by Storm to limit the resource usage of workers to guarantee fairness and QOS.  

**Please note: CGroups is currently supported only on Linux platforms (kernel version 2.6.24 and above)** 

## Setup

To use CGroups make sure to install cgroups and configure cgroups correctly.  For more information about setting up and configuring, please visit:

https://access.redhat.com/documentation/en-US/Red_Hat_Enterprise_Linux/6/html/Resource_Management_Guide/ch-Using_Control_Groups.html

A sample/default cgconfig.conf file is supplied in the <stormroot>/conf directory.  The contents are as follows:

```
mount {
	cpuset	= /cgroup/cpuset;
	cpu	= /cgroup/storm_resources;
	cpuacct	= /cgroup/cpuacct;
	memory	= /cgroup/storm_resources;
	devices	= /cgroup/devices;
	freezer	= /cgroup/freezer;
	net_cls	= /cgroup/net_cls;
	blkio	= /cgroup/blkio;
}

group storm {
       perm {
               task {
                      uid = 500;
                      gid = 500;
               }
               admin {
                      uid = 500;
                      gid = 500;
               }
       }
       cpu {
       }
}
```

For a more detailed explanation of the format and configs for the cgconfig.conf file, please visit:

https://access.redhat.com/documentation/en-US/Red_Hat_Enterprise_Linux/6/html/Resource_Management_Guide/ch-Using_Control_Groups.html#The_cgconfig.conf_File

# Settings Related To CGroups in Storm

| Setting                       | Function                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                            |
|-------------------------------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| storm.cgroup.enable                | This config is used to set whether or not cgroups will be used.  Set "true" to enable use of cgroups.  Set "false" to not use cgroups. When this config is set to false, unit tests related to cgroups will be skipped. Default set to "false"                                                                                                                                                                                                                                                                                         |
| storm.cgroup.hierarchy.dir   | The path to the cgroup hierarchy that storm will use.  Default set to "/cgroup/storm_resources"                                                                                                                                                                                                                                                                                                                                                                                                                                     |
| storm.cgroup.resources       | A list of subsystems that will be regulated by CGroups. Default set to cpu and memory.  Currently only cpu and memory are supported                                                                                                                                                                                                                                                                                                                                                                                                                                               |
| storm.supervisor.cgroup.rootdir     | The root cgroup used by the supervisor.  The path to the cgroup will be \<storm.cgroup.hierarchy.dir>/\<storm.supervisor.cgroup.rootdir>.  Default set to "storm"                                                                                                                                                                                                                                                                                                                                                                           |
| storm.cgroup.cgexec.cmd            | Absolute path to the cgexec command used to launch workers within a cgroup. Default set to "/bin/cgexec"                                                                                                                                                                                                                                                                                                                                                                                                                            |
| storm.worker.cgroup.memory.mb.limit | The memory limit in MB for each worker.  This can be set on a per supervisor node basis.  This config is used to set the cgroup config memory.limit_in_bytes.  For more details about memory.limit_in_bytes, please visit:  https://access.redhat.com/documentation/en-US/Red_Hat_Enterprise_Linux/6/html/Resource_Management_Guide/sec-memory.html.    Please note, if you are using the Resource Aware Scheduler, please do NOT set this config as this config will override the values calculated by the Resource Aware Scheduler |
| storm.worker.cgroup.cpu.limit       | The cpu share for each worker. This can be set on a per supervisor node basis.  This config is used to set the cgroup config cpu.share. For more details about cpu.share, please visit:   https://access.redhat.com/documentation/en-US/Red_Hat_Enterprise_Linux/6/html/Resource_Management_Guide/sec-cpu.html. Please note, if you are using the Resource Aware Scheduler, please do NOT set this config as this config will override the values calculated by the Resource Aware Scheduler.                                       |

Since limiting CPU usage via cpu.shares only limits the proportional CPU usage of a process, to limit the amount of CPU usage of all the worker processes on a supervisor node, please set the config supervisor.cpu.capacity. Where each increment represents 1% of a core thus if a user sets supervisor.cpu.capacity: 200, the user is indicating the use of 2 cores.

## Integration with Resource Aware Scheduler

CGroups can be used in conjunction with the Resource Aware Scheduler.  CGroups will then enforce the resource usage of workers as allocated by the Resource Aware Scheduler.  To use cgroups with the Resource Aware Scheduler, simply enable cgroups and be sure NOT to set storm.worker.cgroup.memory.mb.limit and storm.worker.cgroup.cpu.limit configs.


