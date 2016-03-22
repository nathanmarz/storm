---
title: Pacemaker
layout: documentation
documentation: true
---


### Introduction
Pacemaker is a storm daemon designed to process heartbeats from workers. As Storm is scaled up, ZooKeeper begins to become a bottleneck due to high volumes of writes from workers doing heartbeats. Lots of writes to disk and too much traffic across the network is generated as ZooKeeper tries to maintain consistency.

Because heartbeats are of an ephemeral nature, they do not need to be persisted to disk or synced across nodes; an in-memory store will do. This is the role of Pacemaker. Pacemaker functions as a simple in-memory key/value store with ZooKeeper-like, directory-style keys and byte array values.

The corresponding Pacemaker client is a plugin for the `ClusterState` interface, `org.apache.storm.pacemaker.pacemaker_state_factory`. Heartbeat calls are funneled by the `ClusterState` produced by `pacemaker_state_factory` into the Pacemaker daemon, while other set/get operations are forwarded to ZooKeeper.

------

### Configuration

 - `pacemaker.host` : The host that the Pacemaker daemon is running on
 - `pacemaker.port` : The port that Pacemaker will listen on
 - `pacemaker.max.threads` : Maximum number of threads Pacemaker daemon will use to handle requests.
 - `pacemaker.childopts` : Any JVM parameters that need to go to the Pacemaker. (used by storm-deploy project)
 - `pacemaker.auth.method` : The authentication method that is used (more info below)

#### Example

To get Pacemaker up and running, set the following option in the cluster config on all nodes:
```
storm.cluster.state.store: "org.apache.storm.pacemaker.pacemaker_state_factory"
```

The Pacemaker host also needs to be set on all nodes:
```
pacemaker.host: somehost.mycompany.com
```

And then start all of your daemons

(including Pacemaker):
```
$ storm pacemaker
```

The Storm cluster should now be pushing all worker heartbeats through Pacemaker.

### Security

Currently digest (password-based) and Kerberos security are supported. Security is currently only around reads, not writes. Writes may be performed by anyone, whereas reads may only be performed by authorized and authenticated users. This is an area for future development, as it leaves the cluster open to DoS attacks, but it prevents any sensitive information from reaching unauthorized eyes, which was the main goal.

#### Digest
To configure digest authentication, set `pacemaker.auth.method: DIGEST` in the cluster config on the nodes hosting Nimbus and Pacemaker.
The nodes must also have `java.security.auth.login.config` set to point to a JAAS config file containing the following structure:
```
PacemakerDigest {
    username="some username"
    password="some password";
};
```

Any node with these settings configured will be able to read from Pacemaker.
Worker nodes need not have these configs set, and may keep `pacemaker.auth.method: NONE` set, since they do not need to read from the Pacemaker daemon.

#### Kerberos
To configure Kerberos authentication, set `pacemaker.auth.method: KERBEROS` in the cluster config on the nodes hosting Nimbus and Pacemaker.
The nodes must also have `java.security.auth.login.config` set to point to a JAAS config.

The JAAS config on Nimbus must look something like this:
```
PacemakerClient {
    com.sun.security.auth.module.Krb5LoginModule required
    useKeyTab=true
    keyTab="/etc/keytabs/nimbus.keytab"
    storeKey=true
    useTicketCache=false
    serviceName="pacemaker"
    principal="nimbus@MY.COMPANY.COM";
};
                         
```

The JAAS config on Pacemaker must look something like this:
```
PacemakerServer {
   com.sun.security.auth.module.Krb5LoginModule required
   useKeyTab=true
   keyTab="/etc/keytabs/pacemaker.keytab"
   storeKey=true
   useTicketCache=false
   principal="pacemaker@MY.COMPANY.COM";
};
```

 - The client's user principal in the `PacemakerClient` section on the Nimbus host must match the `nimbus.daemon.user` storm cluster config value.
 - The client's `serviceName` value must match the server's user principal in the `PacemakerServer` section on the Pacemaker host.


### Fault Tolerance

Pacemaker runs as a single daemon instance, making it a potential Single Point of Failure.

If Pacemaker becomes unreachable by Nimbus, through crash or network partition, the workers will continue to run, and Nimbus will repeatedly attempt to reconnect. Nimbus functionality will be disrupted, but the topologies themselves will continue to run.
In case of partition of the cluster where Nimbus and Pacemaker are on the same side of the partition, the workers that are on the other side of the partition will not be able to heartbeat, and Nimbus will reschedule the tasks elsewhere. This is probably what we want to happen anyway.


### ZooKeeper Comparison
Compared to ZooKeeper, Pacemaker uses less CPU, less memory, and of course no disk for the same load, thanks to lack of overhead from maintaining consistency between nodes.
On Gigabit networking, there is a theoretical limit of about 6000 nodes. However, the real limit is likely around 2000-3000 nodes. These limits have not yet been tested.
On a 270 supervisor cluster, fully scheduled with topologies, Pacemaker resource utilization was 70% of one core and nearly 1GiB of RAM on a machine with 4 `Intel(R) Xeon(R) CPU E5530 @ 2.40GHz` and 24GiB of RAM.


There is an easy route to HA for Pacemaker. Unlike ZooKeeper, Pacemaker should be able to scale horizontally without overhead. By contrast, with ZooKeeper, there are diminishing returns when adding ZK nodes.

In short, a single Pacemaker node should be able to handle many times the load that a ZooKeeper cluster can, and future HA work allowing horizontal scaling will increase that even farther.
