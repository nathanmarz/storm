---
layout: documentation
---
Storm comes with a Clojure DSL for defining spouts, bolts, and topologies. The Clojure DSL has access to everything the Java API exposes, so if you're a Clojure user you can code Storm topologies without touching Java at all. The Clojure DSL is defined in the source in the [backtype.storm.clojure](https://github.com/apache/incubator-storm/blob/0.5.3/src/clj/backtype/storm/clojure.clj) namespace.

This page outlines all the pieces of the Clojure DSL, including:

1. Defining topologies
2. `defbolt`
3. `defspout`
4. Running topologies in local mode or on a cluster
5. Testing topologies

### Defining topologies

To define a topology, use the `topology` function. `topology` takes in two arguments: a map of "spout specs" and a map of "bolt specs". Each spout and bolt spec wires the code for the component into the topology by specifying things like inputs and parallelism.

Let's take a look at an example topology definition [from the storm-starter project](https://github.com/nathanmarz/storm-starter/blob/master/src/clj/storm/starter/clj/word_count.clj):

```clojure
(topology
 {"1" (spout-spec sentence-spout)
  "2" (spout-spec (sentence-spout-parameterized
                   ["the cat jumped over the door"
                    "greetings from a faraway land"])
                   :p 2)}
 {"3" (bolt-spec {"1" :shuffle "2" :shuffle}
                 split-sentence
                 :p 5)
  "4" (bolt-spec {"3" ["word"]}
                 word-count
                 :p 6)})
```

The maps of spout and bolt specs are maps from the component id to the corresponding spec. The component ids must be unique across the maps. Just like defining topologies in Java, component ids are used when declaring inputs for bolts in the topology.

#### spout-spec

`spout-spec` takes as arguments the spout implementation (an object that implements [IRichSpout](javadocs/backtype/storm/topology/IRichSpout.html)) and optional keyword arguments. The only option that exists currently is the `:p` option, which specifies the parallelism for the spout. If you omit `:p`, the spout will execute as a single task.

#### bolt-spec

`bolt-spec` takes as arguments the input declaration for the bolt, the bolt implementation (an object that implements [IRichBolt](javadocs/backtype/storm/topology/IRichBolt.html)), and optional keyword arguments.

The input declaration is a map from stream ids to stream groupings. A stream id can have one of two forms:

1. `[==component id== ==stream id==]`: Subscribes to a specific stream on a component
2. `==component id==`: Subscribes to the default stream on a component

A stream grouping can be one of the following:

1. `:shuffle`: subscribes with a shuffle grouping
2. Vector of field names, like `["id" "name"]`: subscribes with a fields grouping on the specified fields
3. `:global`: subscribes with a global grouping
4. `:all`: subscribes with an all grouping
5. `:direct`: subscribes with a direct grouping

See [Concepts](Concepts.html) for more info on stream groupings. Here's an example input declaration showcasing the various ways to declare inputs:

```clojure
{["2" "1"] :shuffle
 "3" ["field1" "field2"]
 ["4" "2"] :global}
```

This input declaration subscribes to three streams total. It subscribes to stream "1" on component "2" with a shuffle grouping, subscribes to the default stream on component "3" with a fields grouping on the fields "field1" and "field2", and subscribes to stream "2" on component "4" with a global grouping.

Like `spout-spec`, the only current supported keyword argument for `bolt-spec` is `:p` which specifies the parallelism for the bolt.

#### shell-bolt-spec

`shell-bolt-spec` is used for defining bolts that are implemented in a non-JVM language. It takes as arguments the input declaration, the command line program to run, the name of the file implementing the bolt, an output specification, and then the same keyword arguments that `bolt-spec` accepts.

Here's an example `shell-bolt-spec`:

```clojure
(shell-bolt-spec {"1" :shuffle "2" ["id"]}
                 "python"
                 "mybolt.py"
                 ["outfield1" "outfield2"]
                 :p 25)
```

The syntax of output declarations is described in more detail in the `defbolt` section below. See [Using non JVM languages with Storm](Using-non-JVM-languages-with-Storm.html) for more details on how multilang works within Storm.

### defbolt

`defbolt` is used for defining bolts in Clojure. Bolts have the constraint that they must be serializable, and this is why you can't just reify `IRichBolt` to implement a bolt (closures aren't serializable). `defbolt` works around this restriction and provides a nicer syntax for defining bolts than just implementing a Java interface.

At its fullest expressiveness, `defbolt` supports parameterized bolts and maintaining state in a closure around the bolt implementation. It also provides shortcuts for defining bolts that don't need this extra functionality. The signature for `defbolt` looks like the following:

(defbolt _name_ _output-declaration_ *_option-map_ & _impl_)

Omitting the option map is equivalent to having an option map of `{:prepare false}`.

#### Simple bolts

Let's start with the simplest form of `defbolt`. Here's an example bolt that splits a tuple containing a sentence into a tuple for each word:

```clojure
(defbolt split-sentence ["word"] [tuple collector]
  (let [words (.split (.getString tuple 0) " ")]
    (doseq [w words]
      (emit-bolt! collector [w] :anchor tuple))
    (ack! collector tuple)
    ))
```

Since the option map is omitted, this is a non-prepared bolt. The DSL simply expects an implementation for the `execute` method of `IRichBolt`. The implementation takes two parameters, the tuple and the `OutputCollector`, and is followed by the body of the `execute` function. The DSL automatically type-hints the parameters for you so you don't need to worry about reflection if you use Java interop.

This implementation binds `split-sentence` to an actual `IRichBolt` object that you can use in topologies, like so:

```clojure
(bolt-spec {"1" :shuffle}
           split-sentence
           :p 5)
```


#### Parameterized bolts

Many times you want to parameterize your bolts with other arguments. For example, let's say you wanted to have a bolt that appends a suffix to every input string it receives, and you want that suffix to be set at runtime. You do this with `defbolt` by including a `:params` option in the option map, like so:

```clojure
(defbolt suffix-appender ["word"] {:params [suffix]}
  [tuple collector]
  (emit-bolt! collector [(str (.getString tuple 0) suffix)] :anchor tuple)
  )
```

Unlike the previous example, `suffix-appender` will be bound to a function that returns an `IRichBolt` rather than be an `IRichBolt` object directly. This is caused by specifying `:params` in its option map. So to use `suffix-appender` in a topology, you would do something like:

```clojure
(bolt-spec {"1" :shuffle}
           (suffix-appender "-suffix")
           :p 10)
```

#### Prepared bolts

To do more complex bolts, such as ones that do joins and streaming aggregations, the bolt needs to store state. You can do this by creating a prepared bolt which is specified by including `{:prepare true}` in the option map. Consider, for example, this bolt that implements word counting:

```clojure
(defbolt word-count ["word" "count"] {:prepare true}
  [conf context collector]
  (let [counts (atom {})]
    (bolt
     (execute [tuple]
       (let [word (.getString tuple 0)]
         (swap! counts (partial merge-with +) {word 1})
         (emit-bolt! collector [word (@counts word)] :anchor tuple)
         (ack! collector tuple)
         )))))
```

The implementation for a prepared bolt is a function that takes as input the topology config, `TopologyContext`, and `OutputCollector`, and returns an implementation of the `IBolt` interface. This design allows you to have a closure around the implementation of `execute` and `cleanup`. 

In this example, the word counts are stored in the closure in a map called `counts`. The `bolt` macro is used to create the `IBolt` implementation. The `bolt` macro is a more concise way to implement the interface than reifying, and it automatically type-hints all of the method parameters. This bolt implements the execute method which updates the count in the map and emits the new word count.

Note that the `execute` method in prepared bolts only takes as input the tuple since the `OutputCollector` is already in the closure of the function (for simple bolts the collector is a second parameter to the `execute` function).

Prepared bolts can be parameterized just like simple bolts.

#### Output declarations

The Clojure DSL has a concise syntax for declaring the outputs of a bolt. The most general way to declare the outputs is as a map from stream id a stream spec. For example:

```clojure
{"1" ["field1" "field2"]
 "2" (direct-stream ["f1" "f2" "f3"])
 "3" ["f1"]}
```

The stream id is a string, while the stream spec is either a vector of fields or a vector of fields wrapped by `direct-stream`. `direct stream` marks the stream as a direct stream (See [Concepts](Concepts.html) and [Direct groupings]() for more details on direct streams).

If the bolt only has one output stream, you can define the default stream of the bolt by using a vector instead of a map for the output declaration. For example:

```clojure
["word" "count"]
```
This declares the output of the bolt as the fields ["word" "count"] on the default stream id.

#### Emitting, acking, and failing

Rather than use the Java methods on `OutputCollector` directly, the DSL provides a nicer set of functions for using `OutputCollector`: `emit-bolt!`, `emit-direct-bolt!`, `ack!`, and `fail!`.

1. `emit-bolt!`: takes as parameters the `OutputCollector`, the values to emit (a Clojure sequence), and keyword arguments for `:anchor` and `:stream`. `:anchor` can be a single tuple or a list of tuples, and `:stream` is the id of the stream to emit to. Omitting the keyword arguments emits an unanchored tuple to the default stream.
2. `emit-direct-bolt!`: takes as parameters the `OutputCollector`, the task id to send the tuple to, the values to emit, and keyword arguments for `:anchor` and `:stream`. This function can only emit to streams declared as direct streams.
2. `ack!`: takes as parameters the `OutputCollector` and the tuple to ack.
3. `fail!`: takes as parameters the `OutputCollector` and the tuple to fail.

See [Guaranteeing message processing](Guaranteeing-message-processing.html) for more info on acking and anchoring.

### defspout

`defspout` is used for defining spouts in Clojure. Like bolts, spouts must be serializable so you can't just reify `IRichSpout` to do spout implementations in Clojure. `defspout` works around this restriction and provides a nicer syntax for defining spouts than just implementing a Java interface.

The signature for `defspout` looks like the following:

(defspout _name_ _output-declaration_ *_option-map_ & _impl_)

If you leave out the option map, it defaults to {:prepare true}. The output declaration for `defspout` has the same syntax as `defbolt`.

Here's an example `defspout` implementation from [storm-starter](https://github.com/nathanmarz/storm-starter/blob/master/src/clj/storm/starter/clj/word_count.clj):

```clojure
(defspout sentence-spout ["sentence"]
  [conf context collector]
  (let [sentences ["a little brown dog"
                   "the man petted the dog"
                   "four score and seven years ago"
                   "an apple a day keeps the doctor away"]]
    (spout
     (nextTuple []
       (Thread/sleep 100)
       (emit-spout! collector [(rand-nth sentences)])         
       )
     (ack [id]
        ;; You only need to define this method for reliable spouts
        ;; (such as one that reads off of a queue like Kestrel)
        ;; This is an unreliable spout, so it does nothing here
        ))))
```

The implementation takes in as input the topology config, `TopologyContext`, and `SpoutOutputCollector`. The implementation returns an `ISpout` object. Here, the `nextTuple` function emits a random sentence from `sentences`. 

This spout isn't reliable, so the `ack` and `fail` methods will never be called. A reliable spout will add a message id when emitting tuples, and then `ack` or `fail` will be called when the tuple is completed or failed respectively. See [Guaranteeing message processing](Guaranteeing-message-processing.html) for more info on how reliability works within Storm.

`emit-spout!` takes in as parameters the `SpoutOutputCollector` and the new tuple to be emitted, and accepts as keyword arguments `:stream` and `:id`. `:stream` specifies the stream to emit to, and `:id` specifies a message id for the tuple (used in the `ack` and `fail` callbacks). Omitting these arguments emits an unanchored tuple to the default output stream.

There is also a `emit-direct-spout!` function that emits a tuple to a direct stream and takes an additional argument as the second parameter of the task id to send the tuple to.

Spouts can be parameterized just like bolts, in which case the symbol is bound to a function returning `IRichSpout` instead of the `IRichSpout` itself. You can also declare an unprepared spout which only defines the `nextTuple` method. Here is an example of an unprepared spout that emits random sentences parameterized at runtime:

```clojure
(defspout sentence-spout-parameterized ["word"] {:params [sentences] :prepare false}
  [collector]
  (Thread/sleep 500)
  (emit-spout! collector [(rand-nth sentences)]))
```

The following example illustrates how to use this spout in a `spout-spec`:

```clojure
(spout-spec (sentence-spout-parameterized
                   ["the cat jumped over the door"
                    "greetings from a faraway land"])
            :p 2)
```

### Running topologies in local mode or on a cluster

That's all there is to the Clojure DSL. To submit topologies in remote mode or local mode, just use the `StormSubmitter` or `LocalCluster` classes just like you would from Java.

To create topology configs, it's easiest to use the `backtype.storm.config` namespace which defines constants for all of the possible configs. The constants are the same as the static constants in the `Config` class, except with dashes instead of underscores. For example, here's a topology config that sets the number of workers to 15 and configures the topology in debug mode:

```clojure
{TOPOLOGY-DEBUG true
 TOPOLOGY-WORKERS 15}
```

### Testing topologies

[This blog post](http://www.pixelmachine.org/2011/12/17/Testing-Storm-Topologies.html) and its [follow-up](http://www.pixelmachine.org/2011/12/21/Testing-Storm-Topologies-Part-2.html) give a good overview of Storm's powerful built-in facilities for testing topologies in Clojure.
