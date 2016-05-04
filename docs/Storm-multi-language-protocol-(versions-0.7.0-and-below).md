---
layout: documentation
---
This page explains the multilang protocol for versions 0.7.0 and below. The protocol changed in version 0.7.1.

# Storm Multi-Language Protocol

## The ShellBolt

Support for multiple languages is implemented via the ShellBolt class.  This
class implements the IBolt interfaces and implements the protocol for
executing a script or program via the shell using Java's ProcessBuilder class.

## Output fields

Output fields are part of the Thrift definition of the topology. This means that when you multilang in Java, you need to create a bolt that extends ShellBolt, implements IRichBolt, and declared the fields in `declareOutputFields`. 
You can learn more about this on [Concepts](Concepts.html)

## Protocol Preamble

A simple protocol is implemented via the STDIN and STDOUT of the executed
script or program. A mix of simple strings and JSON encoded data are exchanged
with the process making support possible for pretty much any language.

# Packaging Your Stuff

To run a ShellBolt on a cluster, the scripts that are shelled out to must be
in the `resources/` directory within the jar submitted to the master.

However, During development or testing on a local machine, the resources
directory just needs to be on the classpath.

## The Protocol

Notes:
* Both ends of this protocol use a line-reading mechanism, so be sure to
trim off newlines from the input and to append them to your output.
* All JSON inputs and outputs are terminated by a single line contained "end".
* The bullet points below are written from the perspective of the script writer's
STDIN and STDOUT.


* Your script will be executed by the Bolt.
* STDIN: A string representing a path. This is a PID directory.
Your script should create an empty file named with it's pid in this directory. e.g.
the PID is 1234, so an empty file named 1234 is created in the directory. This
file lets the supervisor know the PID so it can shutdown the process later on.
* STDOUT: Your PID. This is not JSON encoded, just a string. ShellBolt will log the PID to its log.
* STDIN: (JSON) The Storm configuration.  Various settings and properties.
* STDIN: (JSON) The Topology context
* The rest happens in a while(true) loop
* STDIN: A tuple! This is a JSON encoded structure like this:

```
{
    // The tuple's id
	"id": -6955786537413359385,
	// The id of the component that created this tuple
	"comp": 1,
	// The id of the stream this tuple was emitted to
	"stream": 1,
	// The id of the task that created this tuple
	"task": 9,
	// All the values in this tuple
	"tuple": ["snow white and the seven dwarfs", "field2", 3]
}
```

* STDOUT: The results of your bolt, JSON encoded. This can be a sequence of acks, fails, emits, and/or logs. Emits look like:

```
{
	"command": "emit",
	// The ids of the tuples this output tuples should be anchored to
	"anchors": [1231231, -234234234],
	// The id of the stream this tuple was emitted to. Leave this empty to emit to default stream.
	"stream": 1,
	// If doing an emit direct, indicate the task to sent the tuple to
	"task": 9,
	// All the values in this tuple
	"tuple": ["field1", 2, 3]
}
```

An ack looks like:

```
{
	"command": "ack",
	// the id of the tuple to ack
	"id": 123123
}
```

A fail looks like:

```
{
	"command": "fail",
	// the id of the tuple to fail
	"id": 123123
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

* STDOUT: emit "sync" as a single line by itself when the bolt has finished emitting/acking/failing and is ready for the next input

### sync

Note: This command is not JSON encoded, it is sent as a simple string.

This lets the parent bolt know that the script has finished processing and is ready for another tuple.
