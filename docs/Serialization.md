---
title: Serialization
layout: documentation
documentation: true
---
This page is about how the serialization system in Storm works for versions 0.6.0 and onwards. Storm used a different serialization system prior to 0.6.0 which is documented on [Serialization (prior to 0.6.0)](Serialization-\(prior-to-0.6.0\).html). 

Tuples can be comprised of objects of any types. Since Storm is a distributed system, it needs to know how to serialize and deserialize objects when they're passed between tasks.

Storm uses [Kryo](https://github.com/EsotericSoftware/kryo) for serialization. Kryo is a flexible and fast serialization library that produces small serializations.

By default, Storm can serialize primitive types, strings, byte arrays, ArrayList, HashMap, HashSet, and the Clojure collection types. If you want to use another type in your tuples, you'll need to register a custom serializer.

### Dynamic typing

There are no type declarations for fields in a Tuple. You put objects in fields and Storm figures out the serialization dynamically. Before we get to the interface for serialization, let's spend a moment understanding why Storm's tuples are dynamically typed.

Adding static typing to tuple fields would add large amount of complexity to Storm's API. Hadoop, for example, statically types its keys and values but requires a huge amount of annotations on the part of the user. Hadoop's API is a burden to use and the "type safety" isn't worth it. Dynamic typing is simply easier to use.

Further than that, it's not possible to statically type Storm's tuples in any reasonable way. Suppose a Bolt subscribes to multiple streams. The tuples from all those streams may have different types across the fields. When a Bolt receives a `Tuple` in `execute`, that tuple could have come from any stream and so could have any combination of types. There might be some reflection magic you can do to declare a different method for every tuple stream a bolt subscribes to, but Storm opts for the simpler, straightforward approach of dynamic typing.

Finally, another reason for using dynamic typing is so Storm can be used in a straightforward manner from dynamically typed languages like Clojure and JRuby.

### Custom serialization

As mentioned, Storm uses Kryo for serialization. To implement custom serializers, you need to register new serializers with Kryo. It's highly recommended that you read over [Kryo's home page](https://github.com/EsotericSoftware/kryo) to understand how it handles custom serialization.

Adding custom serializers is done through the "topology.kryo.register" property in your topology config. It takes a list of registrations, where each registration can take one of two forms:

1. The name of a class to register. In this case, Storm will use Kryo's `FieldsSerializer` to serialize the class. This may or may not be optimal for the class -- see the Kryo docs for more details.
2. A map from the name of a class to register to an implementation of [com.esotericsoftware.kryo.Serializer](https://github.com/EsotericSoftware/kryo/blob/master/src/com/esotericsoftware/kryo/Serializer.java).

Let's look at an example.

```
topology.kryo.register:
  - com.mycompany.CustomType1
  - com.mycompany.CustomType2: com.mycompany.serializer.CustomType2Serializer
  - com.mycompany.CustomType3
```

`com.mycompany.CustomType1` and `com.mycompany.CustomType3` will use the `FieldsSerializer`, whereas `com.mycompany.CustomType2` will use `com.mycompany.serializer.CustomType2Serializer` for serialization.

Storm provides helpers for registering serializers in a topology config. The [Config](javadocs/org/apache/storm/Config.html) class has a method called `registerSerialization` that takes in a registration to add to the config.

There's an advanced config called `Config.TOPOLOGY_SKIP_MISSING_KRYO_REGISTRATIONS`. If you set this to true, Storm will ignore any serializations that are registered but do not have their code available on the classpath. Otherwise, Storm will throw errors when it can't find a serialization. This is useful if you run many topologies on a cluster that each have different serializations, but you want to declare all the serializations across all topologies in the `storm.yaml` files.

### Java serialization

If Storm encounters a type for which it doesn't have a serialization registered, it will use Java serialization if possible. If the object can't be serialized with Java serialization, then Storm will throw an error.

Beware that Java serialization is extremely expensive, both in terms of CPU cost as well as the size of the serialized object. It is highly recommended that you register custom serializers when you put the topology in production. The Java serialization behavior is there so that it's easy to prototype new topologies.

You can turn off the behavior to fall back on Java serialization by setting the `Config.TOPOLOGY_FALL_BACK_ON_JAVA_SERIALIZATION` config to false.

### Component-specific serialization registrations

Storm 0.7.0 lets you set component-specific configurations (read more about this at [Configuration](Configuration.html)). Of course, if one component defines a serialization that serialization will need to be available to other bolts -- otherwise they won't be able to receive messages from that component!

When a topology is submitted, a single set of serializations is chosen to be used by all components in the topology for sending messages. This is done by merging the component-specific serializer registrations with the regular set of serialization registrations. If two components define serializers for the same class, one of the serializers is chosen arbitrarily.

To force a serializer for a particular class if there's a conflict between two component-specific registrations, just define the serializer you want to use in the topology-specific configuration. The topology-specific configuration has precedence over component-specific configurations for serialization registrations.
