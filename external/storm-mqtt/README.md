# Storm MQTT Integration

## About

MQTT is a lightweight publish/subscribe protocol frequently used in IoT applications.

Further information can be found at http://mqtt.org

Features include:

* Full MQTT support (e.g. last will, QoS 0-2, retain, etc.)
* Spout implementation(s) for subscribing to MQTT topics
* A bolt implementation for publishing MQTT messages
* A trident function implementation for publishing MQTT messages
* Authentication and TLS/SSL support
* User-defined "mappers" for converting MQTT messages to tuples (subscribers)
* User-defined "mappers" for converting tuples to MQTT messages (publishers)


## Quick Start
To simply see MQTT integration in action, follow the instructions below.

**Start a MQTT broker and publisher**
The command below will create an MQTT broker on port 1883, and start a publsher that will publish random 
temperature/humidity values to an MQTT topic.

Open a terminal and execute the following command (change the path as necessary):

```bash
java -cp examples/target/storm-mqtt-examples-*-SNAPSHOT.jar org.apache.storm.mqtt.examples.MqttBrokerPublisher
```

**Run the example toplogy**
Run the sample topology using Flux. This will start a local mode cluster and topology that consists of the MQTT Spout
publishing to a bolt that simply logs the information it receives.

In a separate terminal, run the following command (Note that the `storm` executable must be on your PATH):

```bash
storm jar ./examples/target/storm-mqtt-examples-*-SNAPSHOT.jar org.apache.storm.flux.Flux ./examples/src/main/flux/sample.yaml --local
```

You should see data from MQTT being logged by the bolt:

```
27020 [Thread-17-log-executor[3 3]] INFO  o.a.s.f.w.b.LogInfoBolt - {user=tgoetz, deviceId=1234, location=office, temperature=67.0, humidity=65.0}
27030 [Thread-17-log-executor[3 3]] INFO  o.a.s.f.w.b.LogInfoBolt - {user=tgoetz, deviceId=1234, location=office, temperature=47.0, humidity=85.0}
27040 [Thread-17-log-executor[3 3]] INFO  o.a.s.f.w.b.LogInfoBolt - {user=tgoetz, deviceId=1234, location=office, temperature=69.0, humidity=94.0}
27049 [Thread-17-log-executor[3 3]] INFO  o.a.s.f.w.b.LogInfoBolt - {user=tgoetz, deviceId=1234, location=office, temperature=4.0, humidity=98.0}
27059 [Thread-17-log-executor[3 3]] INFO  o.a.s.f.w.b.LogInfoBolt - {user=tgoetz, deviceId=1234, location=office, temperature=51.0, humidity=12.0}
27069 [Thread-17-log-executor[3 3]] INFO  o.a.s.f.w.b.LogInfoBolt - {user=tgoetz, deviceId=1234, location=office, temperature=27.0, humidity=65.0}
2
```

Either allow the local cluster to exit, or stop it by typing Cntrl-C.

**MQTT Fault Tolerance In Action**
After the toplogy has been shutdown, the MQTT subscription created by the MQTT spout will persist with the broker,
and it will continue to receive and queue messages (as long as the broker is running).

If you run the toplogy again (while the broker is still running), when the spout initially connects to the MQTT broker,
it will receive all the messages it missed while it was down. You should see this as burst of messages, followed by a 
rate of about two messages per second.

This happens because, by default, the MQTT Spout creates a *session* when it subscribes -- that means it requests that
the broker hold onto and redeliver any messages it missed while offline. Another important factor is the the 
`MqttBrokerPublisher` publishes messages with a MQTT QoS of `1`, meaning *at most once delivery*.

For more information about MQTT fault tolerance, see the **Delivery Guarantees** section below.




## Delivery Guarantees
In Storm terms, ***the MQTT Spout provides at least once delivery***, depending on the configuration of the publisher as
well as the MQTT spout.

The MQTT protocol defines the following QoS levels:

`0` - At Most Once (AKA "Fire and Forget")
`1` - At Least Once
`2` - Exactly Once

This can be a little confusing as the MQTT protocol specification does not really address the concept of a node being 
completely incinerated by a catasrophic event. This is in stark contrast with Storm's reliability model, which expects 
and embraces the concept of node failure.

So resiliancy is ultimately dependent on the underlying MQTT implementation and infrastructure.

**Recommendations**
*You will never get at exactly once processing with this spout. It can be used with Trident, but it won't provide 
transational semantics.*

If you need reliability guarantees (i.e. *at least once processing*):

1. For MQTT publishers (outside of Storm), publish messages with a QoS of `1` so the broker saves messages if/when the 
spout is offline.
2. Use the spout defaults (`cleanSession = false` and `qos = 1`)
3. If you can, make sure any result of receiving and MQTT message is idempotent.
4. Make sure your MQTT brokers don't die or get isolated due to a network partition. Be prepared for natural and 
man-made disasters and network partitions. Incineration and destruction happens.





## Configuration
For the full range of configuration options, see the JavaDoc for `org.apache.storm.mqtt.common.MqttOptions`.

### Using Flux

The following Flux YAML configuration creates the toplolgy used in the example:

```yaml
name: "mqtt-topology"

components:
   ########## MQTT Spout Config ############
  - id: "mqtt-type"
    className: "org.apache.storm.mqtt.examples.CustomMessageMapper"

  - id: "mqtt-options"
    className: "org.apache.storm.mqtt.common.MqttOptions"
    properties:
      - name: "url"
        value: "tcp://localhost:1883"
      - name: "topics"
        value:
          - "/users/tgoetz/#"

# topology configuration
config:
  topology.workers: 1
  topology.max.spout.pending: 1000

# spout definitions
spouts:
  - id: "mqtt-spout"
    className: "org.apache.storm.mqtt.spout.MqttSpout"
    constructorArgs:
      - ref: "mqtt-type"
      - ref: "mqtt-options"
    parallelism: 1

# bolt definitions
bolts:
  - id: "log"
    className: "org.apache.storm.flux.wrappers.bolts.LogInfoBolt"
    parallelism: 1


streams:
  - from: "mqtt-spout"
    to: "log"
    grouping:
      type: SHUFFLE

```


### Using Java

Similarly, you can create the same topology using the Storm Core Java API:

```java
TopologyBuilder builder = new TopologyBuilder();
MqttOptions options = new MqttOptions();
options.setTopics(Arrays.asList("/users/tgoetz/#"));
options.setCleanConnection(false);
MqttSpout spout = new MqttSpout(new StringMessageMapper(), options);

MqttBolt bolt = new LogInfoBolt();

builder.setSpout("mqtt-spout", spout);
builder.setBolt("log-bolt", bolt).shuffleGrouping("mqtt-spout");

return builder.createTopology();
```