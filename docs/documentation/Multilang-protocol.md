---
title: Multi-Lang Protocol
layout: documentation
documentation: true
---
This page explains the multilang protocol as of Storm 0.7.1. Versions prior to 0.7.1 used a somewhat different protocol, documented [here](Storm-multi-language-protocol-(versions-0.7.0-and-below\).html).

# Storm Multi-Language Protocol

## Shell Components

Support for multiple languages is implemented via the ShellBolt,
ShellSpout, and ShellProcess classes.  These classes implement the
IBolt and ISpout interfaces and the protocol for executing a script or
program via the shell using Java's ProcessBuilder class.

## Output fields

Output fields are part of the Thrift definition of the topology. This means that when you multilang in Java, you need to create a bolt that extends ShellBolt, implements IRichBolt, and declare the fields in `declareOutputFields` (similarly for ShellSpout).

You can learn more about this on [Concepts](Concepts.html)

## Protocol Preamble

A simple protocol is implemented via the STDIN and STDOUT of the
executed script or program. All data exchanged with the process is
encoded in JSON, making support possible for pretty much any language.

# Packaging Your Stuff

To run a shell component on a cluster, the scripts that are shelled
out to must be in the `resources/` directory within the jar submitted
to the master.

However, during development or testing on a local machine, the resources
directory just needs to be on the classpath.

## The Protocol

Notes:

* Both ends of this protocol use a line-reading mechanism, so be sure to
trim off newlines from the input and to append them to your output.
* All JSON inputs and outputs are terminated by a single line containing "end". Note that this delimiter is not itself JSON encoded.
* The bullet points below are written from the perspective of the script writer's
STDIN and STDOUT.

### Initial Handshake

The initial handshake is the same for both types of shell components:

* STDIN: Setup info. This is a JSON object with the Storm configuration, a PID directory, and a topology context, like this:

```
{
    "conf": {
        "topology.message.timeout.secs": 3,
        // etc
    },
    "pidDir": "...",
    "context": {
        "task->component": {
            "1": "example-spout",
            "2": "__acker",
            "3": "example-bolt1",
            "4": "example-bolt2"
        },
        "taskid": 3,
        // Everything below this line is only available in Storm 0.10.0+
        "componentid": "example-bolt"
        "stream->target->grouping": {
        	"default": {
        		"example-bolt2": {
        			"type": "SHUFFLE"}}},
        "streams": ["default"],
 		"stream->outputfields": {"default": ["word"]},
	    "source->stream->grouping": {
	    	"example-spout": {
	    		"default": {
	    			"type": "FIELDS",
	    			"fields": ["word"]
	    		}
	    	}
	    }
	    "source->stream->fields": {
	    	"example-spout": {
	    		"default": ["word"]
	    	}
	    }
	}
}
```

Your script should create an empty file named with its PID in this directory. e.g.
the PID is 1234, so an empty file named 1234 is created in the directory. This
file lets the supervisor know the PID so it can shutdown the process later on.

As of Storm 0.10.0, the context sent by Storm to shell components has been
enhanced substantially to include all aspects of the topology context available
to JVM components.  One key addition is the ability to determine a shell
component's source and targets (i.e., inputs and outputs) in the topology via
the `stream->target->grouping` and `source->stream->grouping` dictionaries.  At
the innermost level of these nested dictionaries, groupings are represented as
a dictionary that minimally has a `type` key, but can also have a `fields` key
to specify which fields are involved in a `FIELDS` grouping.

* STDOUT: Your PID, in a JSON object, like `{"pid": 1234}`. The shell component will log the PID to its log.

What happens next depends on the type of component:

### Spouts

Shell spouts are synchronous. The rest happens in a while(true) loop:

* STDIN: Either a next, ack, or fail command.

"next" is the equivalent of ISpout's `nextTuple`. It looks like:

```
{"command": "next"}
```

"ack" looks like:

```
{"command": "ack", "id": "1231231"}
```

"fail" looks like:

```
{"command": "fail", "id": "1231231"}
```

* STDOUT: The results of your spout for the previous command. This can
  be a sequence of emits and logs.

An emit looks like:

```
{
	"command": "emit",
	// The id for the tuple. Leave this out for an unreliable emit. The id can
    // be a string or a number.
	"id": "1231231",
	// The id of the stream this tuple was emitted to. Leave this empty to emit to default stream.
	"stream": "1",
	// If doing an emit direct, indicate the task to send the tuple to
	"task": 9,
	// All the values in this tuple
	"tuple": ["field1", 2, 3]
}
```

If not doing an emit direct, you will immediately receive the task ids to which the tuple was emitted on STDIN as a JSON array.

A "log" will log a message in the worker log. It looks like:

```
{
	"command": "log",
	// the message to log
	"msg": "hello world!"
}
```

* STDOUT: a "sync" command ends the sequence of emits and logs. It looks like:

```
{"command": "sync"}
```

After you sync, ShellSpout will not read your output until it sends another next, ack, or fail command.

Note that, similarly to ISpout, all of the spouts in the worker will be locked up after a next, ack, or fail, until you sync. Also like ISpout, if you have no tuples to emit for a next, you should sleep for a small amount of time before syncing. ShellSpout will not automatically sleep for you.


### Bolts

The shell bolt protocol is asynchronous. You will receive tuples on STDIN as soon as they are available, and you may emit, ack, and fail, and log at any time by writing to STDOUT, as follows:

* STDIN: A tuple! This is a JSON encoded structure like this:

```
{
    // The tuple's id - this is a string to support languages lacking 64-bit precision
	"id": "-6955786537413359385",
	// The id of the component that created this tuple
	"comp": "1",
	// The id of the stream this tuple was emitted to
	"stream": "1",
	// The id of the task that created this tuple
	"task": 9,
	// All the values in this tuple
	"tuple": ["snow white and the seven dwarfs", "field2", 3]
}
```

* STDOUT: An ack, fail, emit, or log. Emits look like:

```
{
	"command": "emit",
	// The ids of the tuples this output tuples should be anchored to
	"anchors": ["1231231", "-234234234"],
	// The id of the stream this tuple was emitted to. Leave this empty to emit to default stream.
	"stream": "1",
	// If doing an emit direct, indicate the task to send the tuple to
	"task": 9,
	// All the values in this tuple
	"tuple": ["field1", 2, 3]
}
```

If not doing an emit direct, you will receive the task ids to which
the tuple was emitted on STDIN as a JSON array. Note that, due to the
asynchronous nature of the shell bolt protocol, when you read after
emitting, you may not receive the task ids. You may instead read the
task ids for a previous emit or a new tuple to process. You will
receive the task id lists in the same order as their corresponding
emits, however.

An ack looks like:

```
{
	"command": "ack",
	// the id of the tuple to ack
	"id": "123123"
}
```

A fail looks like:

```
{
	"command": "fail",
	// the id of the tuple to fail
	"id": "123123"
}
```

A "log" will log a message in the worker log. It looks like:

```
{
	"command": "log",
	// the message to log
	"msg": "hello world!"
}
```

* Note that, as of version 0.7.1, there is no longer any need for a
  shell bolt to 'sync'.

### Handling Heartbeats (0.9.3 and later)

As of Storm 0.9.3, heartbeats have been between ShellSpout/ShellBolt and their
multi-lang subprocesses to detect hanging/zombie subprocesses.  Any libraries
for interfacing with Storm via multi-lang must take the following actions
regarding hearbeats:

#### Spout

Shell spouts are synchronous, so subprocesses always send `sync` commands at the
end of `next()`,  so you should not have to do much to support heartbeats for
spouts.  That said, you must not let subprocesses sleep more than the worker
timeout during `next()`.

#### Bolt

Shell bolts are asynchronous, so a ShellBolt will send heartbeat tuples to its
subprocess periodically.  Heartbeat tuple looks like:

```
{
	"id": "-6955786537413359385",
	"comp": "1",
	"stream": "__heartbeat",
	// this shell bolt's system task id
	"task": -1,
	"tuple": []
}
```

When subprocess receives heartbeat tuple, it must send a `sync` command back to
ShellBolt.
