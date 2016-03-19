---
title: Hooks
layout: documentation
documentation: true
---
Storm provides hooks with which you can insert custom code to run on any number of events within Storm. You create a hook by extending the [BaseTaskHook](javadocs/backtype/storm/hooks/BaseTaskHook.html) class and overriding the appropriate method for the event you want to catch. There are two ways to register your hook:

1. In the open method of your spout or prepare method of your bolt using the [TopologyContext](javadocs/backtype/storm/task/TopologyContext.html#addTaskHook) method.
2. Through the Storm configuration using the ["topology.auto.task.hooks"](javadocs/backtype/storm/Config.html#TOPOLOGY_AUTO_TASK_HOOKS) config. These hooks are automatically registered in every spout or bolt, and are useful for doing things like integrating with a custom monitoring system.
