# Running Apache Storm Securely

The current release of Apache Storm offers no authentication or authorization.
It does not encrypt any data being sent across the network, and does not 
attempt to restrict access to data stored on the local file system or in
Apache Zookeeper.  As such there are a number of different precautions you may
want to enact outside of storm itself to be sure storm is running securely.

The exact detail of how to setup these precautions varies a lot and is beyond
the scope of this document.

## Network Security

It is generally a good idea to enable a firewall and restrict incoming network
connections to only those originating from the cluster itself and from trusted
hosts and services, a complete list of ports storm uses are below. 

If the data your cluster is processing is sensitive it might be best to setup
IPsec to encrypt all traffic being sent between the hosts in the cluster.

### Ports

| Default Port | Storm Config | Client Hosts/Processes | Server |
|--------------|--------------|------------------------|--------|
| 2181 | `storm.zookeeper.port` | Nimbus, Supervisors, and Worker processes | Zookeeper |
| 6627 | `nimbus.thrift.port` | Storm clients, Supervisors, and UI | Nimbus |
| 8080 | `ui.port` | Client Web Browsers | UI |
| 8000 | `logviewer.port` | Client Web Browsers | Logviewer |
| 3772 | `drpc.port` | External DRPC Clients | DRPC |
| 3773 | `drpc.invocations.port` | Worker Processes | DRPC |
| 670{0,1,2,3} | `supervisor.slots.ports` | Worker Processes | Worker Processes |

### UI/Logviewer

The UI and logviewer processes provide a way to not only see what a cluster is
doing, but also manipulate running topologies.  In general these processes should
not be exposed except to users of the cluster.  It is often simplest to restrict
these ports to only accept connections from local hosts, and then front them with another web server,
like Apache httpd, that can authenticate/authorize incoming connections and
proxy the connection to the storm process.  To make this work the ui process must have
logviewer.port set to the port of the proxy in its storm.yaml, while the logviewers
must have it set to the actual port that they are going to bind to.

### Nimbus

Nimbus's Thrift port should be locked down as it can be used to control the entire
cluster including running arbitrary user code on different nodes in the cluster.
Ideally access to it is restricted to nodes within the cluster and possibly some gateway
nodes that allow authorized users to log into them and run storm client commands.

### DRPC

Each DRPC server has two different ports.  The invocations port is accessed by worker
processes within the cluster.  The other port is accessed by external clients that
want to query the topology.  The external port should be restricted to hosts that you
want to be able to do queries.

### Supervisors

Supervisors are only clients they are not servers, and as such don't need special restrictions.

### Workers

Worker processes receive data from each other.  There is the option to encrypt this data using
Blowfish by setting `topology.tuple.serializer` to `backtype.storm.security.serialization.BlowfishTupleSerializer`
and setting `topology.tuple.serializer.blowfish.key` to a secret key you want your topology to use.

### Zookeeper

Zookeeper uses other ports for communications within the ensemble the details of which
are beyond the scope of this document.  You should look at restricting Zookeeper access
as well, because storm does not set up any ACLs for the data it write to Zookeeper.

  
