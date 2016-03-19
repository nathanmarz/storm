---
title: Running Apache Storm Securely
layout: documentation
documentation: true
---

# Running Apache Storm Securely

Apache Storm offers a range of configuration options when trying to secure
your cluster.  By default all authentication and authorization is disabled but 
can be turned on as needed.

## Firewall/OS level Security

You can still have a secure storm cluster without turning on formal
Authentication and Authorization. But to do so usually requires 
configuring your Operating System to restrict the operations that can be done.
This is generally a good idea even if you plan on running your cluster with Auth.

The exact detail of how to setup these precautions varies a lot and is beyond
the scope of this document.

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
| 3774 | `drpc.http.port` | External HTTP DRPC Clients | DRPC |
| 670{0,1,2,3} | `supervisor.slots.ports` | Worker Processes | Worker Processes |


### UI/Logviewer

The UI and logviewer processes provide a way to not only see what a cluster is
doing, but also manipulate running topologies.  In general these processes should
not be exposed except to users of the cluster.

Some form of Authentication is typically required, with using java servlet filters 

```yaml
ui.filter: "filter.class"
ui.filter.params: "param1":"value1"
```
or by restricting the UI/log viewers ports to only accept connections from local
hosts, and then front them with another web server, like Apache httpd, that can
authenticate/authorize incoming connections and
proxy the connection to the storm process.  To make this work the ui process must have
logviewer.port set to the port of the proxy in its storm.yaml, while the logviewers
must have it set to the actual port that they are going to bind to.

The servlet filters are preferred because it allows individual topologies to
specificy who is and who is not allowed to access the pages associated with
them.  

Storm UI can be configured to use AuthenticationFilter from hadoop-auth.
```yaml
ui.filter: "org.apache.hadoop.security.authentication.server.AuthenticationFilter"
ui.filter.params:
   "type": "kerberos"
   "kerberos.principal": "HTTP/nimbus.witzend.com"
   "kerberos.keytab": "/vagrant/keytabs/http.keytab"
   "kerberos.name.rules": "RULE:[2:$1@$0]([jt]t@.*EXAMPLE.COM)s/.*/$MAPRED_USER/ RULE:[2:$1@$0]([nd]n@.*EXAMPLE.COM)s/.*/$HDFS_USER/DEFAULT"
```
make sure to create a principal 'HTTP/{hostname}' (here hostname should be the one where UI daemon runs

Once configured users needs to do kinit before accessing UI.
Ex:
curl  -i --negotiate -u:anyUser  -b ~/cookiejar.txt -c ~/cookiejar.txt  http://storm-ui-hostname:8080/api/v1/cluster/summary

1. Firefox: Goto about:config and search for network.negotiate-auth.trusted-uris double-click to  add value "http://storm-ui-hostname:8080"
2. Google-chrome:  start from command line with: google-chrome --auth-server-whitelist="*storm-ui-hostname" --auth-negotiate-delegate-whitelist="*storm-ui-hostname"   
3. IE:  Configure trusted websites to include "storm-ui-hostname" and allow negotiation for that website 

**Caution**: In AD MIT Keberos setup the key size is bigger than the default UI jetty server request header size. Make sure you set ui.header.buffer.bytes to 65536 in storm.yaml. More details are on [STORM-633](https://issues.apache.org/jira/browse/STORM-633)


## UI / DRPC SSL 

Both UI and DRPC allows users to configure ssl .

### UI

For UI users needs to set following config in storm.yaml. Generating keystores with proper keys and certs should be taken care by the user before this step.

1. ui.https.port 
2. ui.https.keystore.type (example "jks")
3. ui.https.keystore.path (example "/etc/ssl/storm_keystore.jks")
4. ui.https.keystore.password (keystore password)
5. ui.https.key.password (private key password)

optional config 
6. ui.https.truststore.path (example "/etc/ssl/storm_truststore.jks")
7. ui.https.truststore.password (truststore password)
8. ui.https.truststore.type (example "jks")

If users want to setup 2-way auth
9. ui.https.want.client.auth (If this set to true server requests for client certifcate authentication, but keeps the connection if no authentication provided)
10. ui.https.need.client.auth (If this set to true server requires client to provide authentication)




### DRPC
similarly to UI , users need to configure following for DRPC

1. drpc.https.port 
2. drpc.https.keystore.type (example "jks")
3. drpc.https.keystore.path (example "/etc/ssl/storm_keystore.jks")
4. drpc.https.keystore.password (keystore password)
5. drpc.https.key.password (private key password)

optional config 
6. drpc.https.truststore.path (example "/etc/ssl/storm_truststore.jks")
7. drpc.https.truststore.password (truststore password)
8. drpc.https.truststore.type (example "jks")

If users want to setup 2-way auth
9. drpc.https.want.client.auth (If this set to true server requests for client certifcate authentication, but keeps the connection if no authentication provided)
10. drpc.https.need.client.auth (If this set to true server requires client to provide authentication)





## Authentication (Kerberos)

Storm offers pluggable authentication support through thrift and SASL.  This
example only goes off of Kerberos as it is a common setup for most big data
projects.

Setting up a KDC and configuring kerberos on each node is beyond the scope of
this document and it is assumed that you have done that already.

### Create Headless Principals and keytabs

Each Zookeeper Server, Nimbus, and DRPC server will need a service principal, which, by convention, includes the FQDN of the host it will run on.  Be aware that the zookeeper user *MUST* be zookeeper.  
The supervisors and UI also need a principal to run as, but because they are outgoing connections they do not need to be service principals. 
The following is an example of how to setup kerberos principals, but the
details may vary depending on your KDC and OS.


```bash
# Zookeeper (Will need one of these for each box in teh Zk ensamble)
sudo kadmin.local -q 'addprinc zookeeper/zk1.example.com@STORM.EXAMPLE.COM'
sudo kadmin.local -q "ktadd -k /tmp/zk.keytab  zookeeper/zk1.example.com@STORM.EXAMPLE.COM"
# Nimbus and DRPC
sudo kadmin.local -q 'addprinc storm/storm.example.com@STORM.EXAMPLE.COM'
sudo kadmin.local -q "ktadd -k /tmp/storm.keytab storm/storm.example.com@STORM.EXAMPLE.COM"
# All UI logviewer and Supervisors
sudo kadmin.local -q 'addprinc storm@STORM.EXAMPLE.COM'
sudo kadmin.local -q "ktadd -k /tmp/storm.keytab storm@STORM.EXAMPLE.COM"
```

be sure to distribute the keytab(s) to the appropriate boxes and set the FS permissions so that only the headless user running ZK, or storm has access to them.

#### Storm Kerberos Configuration

Both storm and Zookeeper use jaas configuration files to log the user in.
Each jaas file may have multiple sections for different interfaces being used.

To enable Kerberos authentication in storm you need to set the following storm.yaml configs
```yaml
storm.thrift.transport: "backtype.storm.security.auth.kerberos.KerberosSaslTransportPlugin"
java.security.auth.login.config: "/path/to/jaas.conf"
```

Nimbus and the supervisor processes will also connect to ZooKeeper(ZK) and we want to configure them to use Kerberos for authentication with ZK. To do this append 
```
-Djava.security.auth.login.config=/path/to/jaas.conf
```

to the childopts of nimbus, ui, and supervisor.  Here is an example given the default childopts settings at the time of writing:

```yaml
nimbus.childopts: "-Xmx1024m -Djava.security.auth.login.config=/path/to/jaas.conf"
ui.childopts: "-Xmx768m -Djava.security.auth.login.config=/path/to/jaas.conf"
supervisor.childopts: "-Xmx256m -Djava.security.auth.login.config=/path/to/jaas.conf"
```

The jaas.conf file should look something like the following for the storm nodes.
The StormServer section is used by nimbus and the DRPC Nodes.  It does not need to be included on supervisor nodes.
The StormClient section is used by all storm clients that want to talk to nimbus, including the ui, logviewer, and supervisor.  We will use this section on the gateways as well but the structure of that will be a bit different.
The Client section is used by processes wanting to talk to zookeeper and really only needs to be included with nimbus and the supervisors.
The Server section is used by the zookeeper servers.
Having unused sections in the jaas is not a problem.

```
StormServer {
   com.sun.security.auth.module.Krb5LoginModule required
   useKeyTab=true
   keyTab="$keytab"
   storeKey=true
   useTicketCache=false
   principal="$principal";
};
StormClient {
   com.sun.security.auth.module.Krb5LoginModule required
   useKeyTab=true
   keyTab="$keytab"
   storeKey=true
   useTicketCache=false
   serviceName="$nimbus_user"
   principal="$principal";
};
Client {
   com.sun.security.auth.module.Krb5LoginModule required
   useKeyTab=true
   keyTab="$keytab"
   storeKey=true
   useTicketCache=false
   serviceName="zookeeper"
   principal="$principal";
};
Server {
   com.sun.security.auth.module.Krb5LoginModule required
   useKeyTab=true
   keyTab="$keytab"
   storeKey=true
   useTicketCache=false
   principal="$principal";
};
```

The following is an example based off of the keytabs generated
```
StormServer {
   com.sun.security.auth.module.Krb5LoginModule required
   useKeyTab=true
   keyTab="/keytabs/storm.keytab"
   storeKey=true
   useTicketCache=false
   principal="storm/storm.example.com@STORM.EXAMPLE.COM";
};
StormClient {
   com.sun.security.auth.module.Krb5LoginModule required
   useKeyTab=true
   keyTab="/keytabs/storm.keytab"
   storeKey=true
   useTicketCache=false
   serviceName="storm"
   principal="storm@STORM.EXAMPLE.COM";
};
Client {
   com.sun.security.auth.module.Krb5LoginModule required
   useKeyTab=true
   keyTab="/keytabs/storm.keytab"
   storeKey=true
   useTicketCache=false
   serviceName="zookeeper"
   principal="storm@STORM.EXAMPLE.COM";
};
Server {
   com.sun.security.auth.module.Krb5LoginModule required
   useKeyTab=true
   keyTab="/keytabs/zk.keytab"
   storeKey=true
   useTicketCache=false
   serviceName="zookeeper"
   principal="zookeeper/zk1.example.com@STORM.EXAMPLE.COM";
};
```

Nimbus also will translate the principal into a local user name, so that other services can use this name.  To configure this for Kerberos authentication set

```
storm.principal.tolocal: "backtype.storm.security.auth.KerberosPrincipalToLocal"
```

This only needs to be done on nimbus, but it will not hurt on any node.
We also need to inform the topology who the supervisor daemon and the nimbus daemon are running as from a ZooKeeper perspective.

```
storm.zookeeper.superACL: "sasl:${nimbus-user}"
```

Here *nimbus-user* is the Kerberos user that nimbus uses to authenticate with ZooKeeper.  If ZooKeeeper is stripping host and realm then this needs to have host and realm stripped too.

#### ZooKeeper Ensemble

Complete details of how to setup a secure ZK are beyond the scope of this document.  But in general you want to enable SASL authentication on each server, and optionally strip off host and realm

```
authProvider.1 = org.apache.zookeeper.server.auth.SASLAuthenticationProvider
kerberos.removeHostFromPrincipal = true
kerberos.removeRealmFromPrincipal = true
```

And you want to include the jaas.conf on the command line when launching the server so it can use it can find the keytab.
```
-Djava.security.auth.login.config=/jaas/zk_jaas.conf
```

#### Gateways

Ideally the end user will only need to run kinit before interacting with storm.  To make this happen seamlessly we need the default jaas.conf on the gateways to be something like

```
StormClient {
   com.sun.security.auth.module.Krb5LoginModule required
   doNotPrompt=false
   useTicketCache=true
   serviceName="$nimbus_user";
};
```

The end user can override this if they have a headless user that has a keytab.

### Authorization Setup

*Authentication* does the job of verifying who the user is, but we also need *authorization* to do the job of enforcing what each user can do.

The preferred authorization plug-in for nimbus is The *SimpleACLAuthorizer*.  To use the *SimpleACLAuthorizer*, set the following:

```yaml
nimbus.authorizer: "backtype.storm.security.auth.authorizer.SimpleACLAuthorizer"
```

DRPC has a separate authorizer configuration for it.  Do not use SimpleACLAuthorizer for DRPC.

The *SimpleACLAuthorizer* plug-in needs to know who the supervisor users are, and it needs to know about all of the administrator users, including the user running the ui daemon. 

These are set through *nimbus.supervisor.users* and *nimbus.admins* respectively.  Each can either be a full Kerberos principal name, or the name of the user with host and realm stripped off.

The Log servers have their own authorization configurations.  These are set through *logs.users* and *logs.groups*.  These should be set to the admin users or groups for all of the nodes in the cluster.  

When a topology is submitted, the submitting user can specify users in this list as well.  The users and groups specified-in addition to the users in the cluster-wide setting-will be granted access to the submitted topology's worker logs in the logviewers.

### Supervisors headless User and group Setup

To ensure isolation of users in multi-tenancy, there is need to run supervisors and headless user and group unique to execution on the supervisor nodes.  To enable this follow below steps.
1. Add headlessuser to all supervisor hosts.
2. Create unique group and make it the primary group for the headless user on the supervisor nodes.
3. The set following properties on storm for these supervisor nodes.

### Multi-tenant Scheduler

To support multi-tenancy better we have written a new scheduler.  To enable this scheduler set.
```yaml
storm.scheduler: "backtype.storm.scheduler.multitenant.MultitenantScheduler"
```
Be aware that many of the features of this scheduler rely on storm authentication.  Without them the scheduler will not know what the user is and will not isolate topologies properly.

The goal of the multi-tenant scheduler is to provide a way to isolate topologies from one another, but to also limit the resources that an individual user can have in the cluster.

The scheduler currently has one config that can be set either through =storm.yaml= or through a separate config file called =multitenant-scheduler.yaml= that should be placed in the same directory as =storm.yaml=.  It is preferable to use =multitenant-scheduler.yaml= because it can be updated without needing to restart nimbus.

There is currently only one config in =multitenant-scheduler.yaml=, =multitenant.scheduler.user.pools= is a map from the user name, to the maximum number of nodes that user is guaranteed to be able to use for their topologies.

For example:

```yaml
multitenant.scheduler.user.pools: 
    "evans": 10
    "derek": 10
```

### Run worker processes as user who submitted the topology
By default storm runs workers as the user that is running the supervisor.  This is not ideal for security.  To make storm run the topologies as the user that launched them set.

```yaml
supervisor.run.worker.as.user: true
```

There are several files that go along with this that are needed to be configured properly to make storm secure.

The worker-launcher executable is a special program that allows the supervisor to launch workers as different users.  For this to work it needs to be owned by root, but with the group set to be a group that only teh supervisor headless user is a part of.
It also needs to have 6550 permissions.
There is also a worker-launcher.cfg file, usually located under /etc/ that should look something like the following

```
storm.worker-launcher.group=$(worker_launcher_group)
min.user.id=$(min_user_id)
```
where worker_launcher_group is the same group the supervisor is a part of, and min.user.id is set to the first real user id on the system.
This config file also needs to be owned by root and not have world or group write permissions.

### Impersonating a user
A storm client may submit requests on behalf of another user. For example, if a `userX` submits an oozie workflow and as part of workflow execution if user `oozie` wants to submit a topology on behalf of `userX`
it can do so by leveraging the impersonation feature.In order to submit topology as some other user , you can use `StormSubmitter.submitTopologyAs` API. Alternatively you can use `NimbusClient.getConfiguredClientAs` 
to get a nimbus client as some other user and perform any nimbus action(i.e. kill/rebalance/activate/deactivate) using this client. 

To ensure only authorized users can perform impersonation you should start nimbus with `nimbus.impersonation.authorizer` set to `backtype.storm.security.auth.authorizer.ImpersonationAuthorizer`. 
The `ImpersonationAuthorizer` uses `nimbus.impersonation.acl` as the acl to authorize users. Following is a sample nimbus config for supporting impersonation:

```yaml
nimbus.impersonation.authorizer: backtype.storm.security.auth.authorizer.ImpersonationAuthorizer
nimbus.impersonation.acl:
    impersonating_user1:
        hosts:
            [comma separated list of hosts from which impersonating_user1 is allowed to impersonate other users]
        groups:
            [comma separated list of groups whose users impersonating_user1 is allowed to impersonate]
    impersonating_user2:
        hosts:
            [comma separated list of hosts from which impersonating_user2 is allowed to impersonate other users]
        groups:
            [comma separated list of groups whose users impersonating_user2 is allowed to impersonate]
```

To support the oozie use case following config can be supplied:
```yaml
nimbus.impersonation.acl:
    oozie:
        hosts:
            [oozie-host1, oozie-host2, 127.0.0.1]
        groups:
            [some-group-that-userX-is-part-of]
```

### Automatic Credentials Push and Renewal
Individual topologies have the ability to push credentials (tickets and tokens) to workers so that they can access secure services.  Exposing this to all of the users can be a pain for them.
To hide this from them in the common case plugins can be used to populate the credentials, unpack them on the other side into a java Subject, and also allow Nimbus to renew the credentials if needed.
These are controlled by the following configs. topology.auto-credentials is a list of java plugins, all of which must implement IAutoCredentials interface, that populate the credentials on gateway 
and unpack them on the worker side. On a kerberos secure cluster they should be set by default to point to backtype.storm.security.auth.kerberos.AutoTGT.  
nimbus.credential.renewers.classes should also be set to this value so that nimbus can periodically renew the TGT on behalf of the user.

nimbus.credential.renewers.freq.secs controls how often the renewer will poll to see if anything needs to be renewed, but the default should be fine.

In addition Nimbus itself can be used to get credentials on behalf of the user submitting topologies. This can be configures using nimbus.autocredential.plugins.classes which is a list 
of fully qualified class names ,all of which must implement INimbusCredentialPlugin.  Nimbus will invoke the populateCredentials method of all the configured implementation as part of topology
submission. You should use this config with topology.auto-credentials and nimbus.credential.renewers.classes so the credentials can be populated on worker side and nimbus can automatically renew
them. Currently there are 2 examples of using this config, AutoHDFS and AutoHBase which auto populates hdfs and hbase delegation tokens for topology submitter so they don't have to distribute keytabs
on all possible worker hosts.

### Limits
By default storm allows any sized topology to be submitted. But ZK and others have limitations on how big a topology can actually be.  The following configs allow you to limit the maximum size a topology can be.

| YAML Setting | Description |
|------------|----------------------|
| nimbus.slots.perTopology | The maximum number of slots/workers a topology can use. |
| nimbus.executors.perTopology | The maximum number of executors/threads a topology can use. |

### Log Cleanup
The Logviewer daemon now is also responsible for cleaning up old log files for dead topologies.

| YAML Setting | Description |
|--------------|-------------------------------------|
| logviewer.cleanup.age.mins | How old (by last modification time) must a worker's log be before that log is considered for clean-up. (Living workers' logs are never cleaned up by the logviewer: Their logs are rolled via logback.) |
| logviewer.cleanup.interval.secs | Interval of time in seconds that the logviewer cleans up worker logs. |


### Allowing specific users or groups to access storm

 With SimpleACLAuthorizer any user with valid kerberos ticket can deploy a topology or do further operations such as activate, deactivate , access cluster information.
 One can restrict this access by specifying nimbus.users or nimbus.groups. If nimbus.users configured only the users in the list can deploy a topology or access cluster.
 Similarly nimbus.groups restrict storm cluster access to users who belong to those groups.
 
 To configure specify the following config in storm.yaml

```yaml
nimbus.users: 
   - "testuser"
```

or 

```yaml
nimbus.groups: 
   - "storm"
```
 

### DRPC
Hopefully more on this soon


