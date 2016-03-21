# Storm HDFS

Storm components for interacting with HDFS file systems
 - HDFS Bolt 
 - HDFS Spout

---

# HDFS Bolt
## Usage
The following example will write pipe("|")-delimited files to the HDFS path hdfs://localhost:54310/foo. After every
1,000 tuples it will sync filesystem, making that data visible to other HDFS clients. It will rotate files when they
reach 5 megabytes in size.

```java
// use "|" instead of "," for field delimiter
RecordFormat format = new DelimitedRecordFormat()
        .withFieldDelimiter("|");

// sync the filesystem after every 1k tuples
SyncPolicy syncPolicy = new CountSyncPolicy(1000);

// rotate files when they reach 5MB
FileRotationPolicy rotationPolicy = new FileSizeRotationPolicy(5.0f, Units.MB);

FileNameFormat fileNameFormat = new DefaultFileNameFormat()
        .withPath("/foo/");

HdfsBolt bolt = new HdfsBolt()
        .withFsUrl("hdfs://localhost:54310")
        .withFileNameFormat(fileNameFormat)
        .withRecordFormat(format)
        .withRotationPolicy(rotationPolicy)
        .withSyncPolicy(syncPolicy);
```


### Packaging a Topology
When packaging your topology, it's important that you use the [maven-shade-plugin]() as opposed to the
[maven-assembly-plugin]().

The shade plugin provides facilities for merging JAR manifest entries, which the hadoop client leverages for URL scheme
resolution.

If you experience errors such as the following:

```
java.lang.RuntimeException: Error preparing HdfsBolt: No FileSystem for scheme: hdfs
```

it's an indication that your topology jar file isn't packaged properly.

If you are using maven to create your topology jar, you should use the following `maven-shade-plugin` configuration to
create your topology jar:

```xml
<plugin>
    <groupId>org.apache.maven.plugins</groupId>
    <artifactId>maven-shade-plugin</artifactId>
    <version>1.4</version>
    <configuration>
        <createDependencyReducedPom>true</createDependencyReducedPom>
    </configuration>
    <executions>
        <execution>
            <phase>package</phase>
            <goals>
                <goal>shade</goal>
            </goals>
            <configuration>
                <transformers>
                    <transformer
                            implementation="org.apache.maven.plugins.shade.resource.ServicesResourceTransformer"/>
                    <transformer
                            implementation="org.apache.maven.plugins.shade.resource.ManifestResourceTransformer">
                        <mainClass></mainClass>
                    </transformer>
                </transformers>
            </configuration>
        </execution>
    </executions>
</plugin>

```

### Specifying a Hadoop Version
By default, storm-hdfs uses the following Hadoop dependencies:

```xml
<dependency>
    <groupId>org.apache.hadoop</groupId>
    <artifactId>hadoop-client</artifactId>
    <version>2.6.1</version>
    <exclusions>
        <exclusion>
            <groupId>org.slf4j</groupId>
            <artifactId>slf4j-log4j12</artifactId>
        </exclusion>
    </exclusions>
</dependency>
<dependency>
    <groupId>org.apache.hadoop</groupId>
    <artifactId>hadoop-hdfs</artifactId>
    <version>2.6.1</version>
    <exclusions>
        <exclusion>
            <groupId>org.slf4j</groupId>
            <artifactId>slf4j-log4j12</artifactId>
        </exclusion>
    </exclusions>
</dependency>
```

If you are using a different version of Hadoop, you should exclude the Hadoop libraries from the storm-hdfs dependency
and add the dependencies for your preferred version in your pom.

Hadoop client version incompatibilites can manifest as errors like:

```
com.google.protobuf.InvalidProtocolBufferException: Protocol message contained an invalid tag (zero)
```

## HDFS Bolt Customization

### Record Formats
Record format can be controlled by providing an implementation of the `org.apache.storm.hdfs.format.RecordFormat`
interface:

```java
public interface RecordFormat extends Serializable {
    byte[] format(Tuple tuple);
}
```

The provided `org.apache.storm.hdfs.format.DelimitedRecordFormat` is capable of producing formats such as CSV and
tab-delimited files.


### File Naming
File naming can be controlled by providing an implementation of the `org.apache.storm.hdfs.format.FileNameFormat`
interface:

```java
public interface FileNameFormat extends Serializable {
    void prepare(Map conf, TopologyContext topologyContext);
    String getName(long rotation, long timeStamp);
    String getPath();
}
```

The provided `org.apache.storm.hdfs.format.DefaultFileNameFormat`  will create file names with the following format:

     {prefix}{componentId}-{taskId}-{rotationNum}-{timestamp}{extension}

For example:

     MyBolt-5-7-1390579837830.txt

By default, prefix is empty and extenstion is ".txt".



### Sync Policies
Sync policies allow you to control when buffered data is flushed to the underlying filesystem (thus making it available
to clients reading the data) by implementing the `org.apache.storm.hdfs.sync.SyncPolicy` interface:

```java
public interface SyncPolicy extends Serializable {
    boolean mark(Tuple tuple, long offset);
    void reset();
}
```
The `HdfsBolt` will call the `mark()` method for every tuple it processes. Returning `true` will trigger the `HdfsBolt`
to perform a sync/flush, after which it will call the `reset()` method.

The `org.apache.storm.hdfs.sync.CountSyncPolicy` class simply triggers a sync after the specified number of tuples have
been processed.

### File Rotation Policies
Similar to sync policies, file rotation policies allow you to control when data files are rotated by providing a
`org.apache.storm.hdfs.rotation.FileRotation` interface:

```java
public interface FileRotationPolicy extends Serializable {
    boolean mark(Tuple tuple, long offset);
    void reset();
    FileRotationPolicy copy();
}
``` 

The `org.apache.storm.hdfs.rotation.FileSizeRotationPolicy` implementation allows you to trigger file rotation when
data files reach a specific file size:

```java
FileRotationPolicy rotationPolicy = new FileSizeRotationPolicy(5.0f, Units.MB);
```

### File Rotation Actions
Both the HDFS bolt and Trident State implementation allow you to register any number of `RotationAction`s.
What `RotationAction`s do is provide a hook to allow you to perform some action right after a file is rotated. For
example, moving a file to a different location or renaming it.


```java
public interface RotationAction extends Serializable {
    void execute(FileSystem fileSystem, Path filePath) throws IOException;
}
```

Storm-HDFS includes a simple action that will move a file after rotation:

```java
public class MoveFileAction implements RotationAction {
    private static final Logger LOG = LoggerFactory.getLogger(MoveFileAction.class);

    private String destination;

    public MoveFileAction withDestination(String destDir){
        destination = destDir;
        return this;
    }

    @Override
    public void execute(FileSystem fileSystem, Path filePath) throws IOException {
        Path destPath = new Path(destination, filePath.getName());
        LOG.info("Moving file {} to {}", filePath, destPath);
        boolean success = fileSystem.rename(filePath, destPath);
        return;
    }
}
```

If you are using Trident and sequence files you can do something like this:

```java
        HdfsState.Options seqOpts = new HdfsState.SequenceFileOptions()
                .withFileNameFormat(fileNameFormat)
                .withSequenceFormat(new DefaultSequenceFormat("key", "data"))
                .withRotationPolicy(rotationPolicy)
                .withFsUrl("hdfs://localhost:54310")
                .addRotationAction(new MoveFileAction().withDestination("/dest2/"));
```

### Data Partitioning
Data can be partitioned to different HDFS directories based on characteristics of the tuple being processed or purely
external factors, such as system time.  To partition your your data, write a class that implements the ```Partitioner```
interface and pass it to the withPartitioner() method of your bolt. The getPartitionPath() method returns a partition
path for a given tuple.

Here's an example of a Partitioner that operates on a specific field of data:

```java

    Partitioner partitoner = new Partitioner() {
            @Override
            public String getPartitionPath(Tuple tuple) {
                return Path.SEPARATOR + tuple.getStringByField("city");
            }
     };
```

## HDFS Bolt Support for HDFS Sequence Files

The `org.apache.storm.hdfs.bolt.SequenceFileBolt` class allows you to write storm data to HDFS sequence files:

```java
        // sync the filesystem after every 1k tuples
        SyncPolicy syncPolicy = new CountSyncPolicy(1000);

        // rotate files when they reach 5MB
        FileRotationPolicy rotationPolicy = new FileSizeRotationPolicy(5.0f, Units.MB);

        FileNameFormat fileNameFormat = new DefaultFileNameFormat()
                .withExtension(".seq")
                .withPath("/data/");

        // create sequence format instance.
        DefaultSequenceFormat format = new DefaultSequenceFormat("timestamp", "sentence");

        SequenceFileBolt bolt = new SequenceFileBolt()
                .withFsUrl("hdfs://localhost:54310")
                .withFileNameFormat(fileNameFormat)
                .withSequenceFormat(format)
                .withRotationPolicy(rotationPolicy)
                .withSyncPolicy(syncPolicy)
                .withCompressionType(SequenceFile.CompressionType.RECORD)
                .withCompressionCodec("deflate");
```

The `SequenceFileBolt` requires that you provide a `org.apache.storm.hdfs.bolt.format.SequenceFormat` that maps tuples to
key/value pairs:

```java
public interface SequenceFormat extends Serializable {
    Class keyClass();
    Class valueClass();

    Writable key(Tuple tuple);
    Writable value(Tuple tuple);
}
```

## HDFS Bolt Support for Avro Files

The `org.apache.storm.hdfs.bolt.AvroGenericRecordBolt` class allows you to write Avro objects directly to HDFS:
 
```java
        // sync the filesystem after every 1k tuples
        SyncPolicy syncPolicy = new CountSyncPolicy(1000);

        // rotate files when they reach 5MB
        FileRotationPolicy rotationPolicy = new FileSizeRotationPolicy(5.0f, Units.MB);

        FileNameFormat fileNameFormat = new DefaultFileNameFormat()
                .withExtension(".avro")
                .withPath("/data/");

        // create sequence format instance.
        DefaultSequenceFormat format = new DefaultSequenceFormat("timestamp", "sentence");

        AvroGenericRecordBolt bolt = new AvroGenericRecordBolt()
                .withFsUrl("hdfs://localhost:54310")
                .withFileNameFormat(fileNameFormat)
                .withRotationPolicy(rotationPolicy)
                .withSyncPolicy(syncPolicy);
```

The avro bolt will write records to separate files based on the schema of the record being processed.  In other words,
if the bolt receives records with two different schemas, it will write to two separate files.  Each file will be rotatated
in accordance with the specified rotation policy. If a large number of Avro schemas are expected, then the bolt should
be configured with a maximum number of open files at least equal to the number of schemas expected to prevent excessive
file open/close/create operations.

To use this bolt you **must** register the appropriate Kryo serializers with your topology configuration.  A convenience
method is provided for this:

`AvroGenericRecordBolt.addAvroKryoSerializations(conf);`

By default Storm will use the ```GenericAvroSerializer``` to handle serialization.  This will work, but there are much 
faster options available if you can pre-define the schemas you will be using or utilize an external schema registry. An
implementation using the Confluent Schema Registry is provided, but others can be implemented and provided to Storm.
Please see the javadoc for classes in org.apache.storm.hdfs.avro for information about using the built-in options or
creating your own.


## HDFS Bolt support for Trident API
storm-hdfs also includes a Trident `state` implementation for writing data to HDFS, with an API that closely mirrors
that of the bolts.

 ```java
         Fields hdfsFields = new Fields("field1", "field2");

         FileNameFormat fileNameFormat = new DefaultFileNameFormat()
                 .withPath("/trident")
                 .withPrefix("trident")
                 .withExtension(".txt");

         RecordFormat recordFormat = new DelimitedRecordFormat()
                 .withFields(hdfsFields);

         FileRotationPolicy rotationPolicy = new FileSizeRotationPolicy(5.0f, FileSizeRotationPolicy.Units.MB);

        HdfsState.Options options = new HdfsState.HdfsFileOptions()
                .withFileNameFormat(fileNameFormat)
                .withRecordFormat(recordFormat)
                .withRotationPolicy(rotationPolicy)
                .withFsUrl("hdfs://localhost:54310");

         StateFactory factory = new HdfsStateFactory().withOptions(options);

         TridentState state = stream
                 .partitionPersist(factory, hdfsFields, new HdfsUpdater(), new Fields());
 ```

 To use the sequence file `State` implementation, use the `HdfsState.SequenceFileOptions`:

 ```java
        HdfsState.Options seqOpts = new HdfsState.SequenceFileOptions()
                .withFileNameFormat(fileNameFormat)
                .withSequenceFormat(new DefaultSequenceFormat("key", "data"))
                .withRotationPolicy(rotationPolicy)
                .withFsUrl("hdfs://localhost:54310")
                .addRotationAction(new MoveFileAction().toDestination("/dest2/"));
```

### Note
Whenever a batch is replayed by storm (due to failures), the trident state implementation automatically removes 
duplicates from the current data file by copying the data up to the last transaction to another file. Since this 
operation involves a lot of data copy, ensure that the data files are rotated at reasonable sizes with `FileSizeRotationPolicy` 
and at reasonable intervals with `TimedRotationPolicy` so that the recovery can complete within topology.message.timeout.secs.

Also note with `TimedRotationPolicy` the files are never rotated in the middle of a batch even if the timer ticks, 
but only when a batch completes so that complete batches can be efficiently recovered in case of failures.

##Working with Secure HDFS
If your topology is going to interact with secure HDFS, your bolts/states needs to be authenticated by NameNode. We 
currently have 2 options to support this:

### Using HDFS delegation tokens 
Your administrator can configure nimbus to automatically get delegation tokens on behalf of the topology submitter user.
The nimbus need to start with following configurations:

nimbus.autocredential.plugins.classes : ["org.apache.storm.hdfs.common.security.AutoHDFS"] 
nimbus.credential.renewers.classes : ["org.apache.storm.hdfs.common.security.AutoHDFS"] 
hdfs.keytab.file: "/path/to/keytab/on/nimbus" (This is the keytab of hdfs super user that can impersonate other users.)
hdfs.kerberos.principal: "superuser@EXAMPLE.com" 
nimbus.credential.renewers.freq.secs : 82800 (23 hours, hdfs tokens needs to be renewed every 24 hours so this value should be
less then 24 hours.)
topology.hdfs.uri:"hdfs://host:port" (This is an optional config, by default we will use value of "fs.defaultFS" property
specified in hadoop's core-site.xml)

Your topology configuration should have:
topology.auto-credentials :["org.apache.storm.hdfs.common.security.AutoHDFS"] 

If nimbus did not have the above configuration you need to add it and then restart it. Ensure the hadoop configuration 
files(core-site.xml and hdfs-site.xml) and the storm-hdfs jar with all the dependencies is present in nimbus's classpath. 
Nimbus will use the keytab and principal specified in the config to authenticate with Namenode. From then on for every
topology submission, nimbus will impersonate the topology submitter user and acquire delegation tokens on behalf of the
topology submitter user. If topology was started with topology.auto-credentials set to AutoHDFS, nimbus will push the
delegation tokens to all the workers for your topology and the hdfs bolt/state will authenticate with namenode using 
these tokens.

As nimbus is impersonating topology submitter user, you need to ensure the user specified in hdfs.kerberos.principal 
has permissions to acquire tokens on behalf of other users. To achieve this you need to follow configuration directions 
listed on this link
http://hadoop.apache.org/docs/current/hadoop-project-dist/hadoop-common/Superusers.html

You can read about setting up secure HDFS here: http://hadoop.apache.org/docs/current/hadoop-project-dist/hadoop-common/SecureMode.html.

### Using keytabs on all worker hosts
If you have distributed the keytab files for hdfs user on all potential worker hosts then you can use this method. You should specify a 
hdfs config key using the method HdfsBolt/State.withconfigKey("somekey") and the value map of this key should have following 2 properties:

hdfs.keytab.file: "/path/to/keytab/"
hdfs.kerberos.principal: "user@EXAMPLE.com"

On worker hosts the bolt/trident-state code will use the keytab file with principal provided in the config to authenticate with 
Namenode. This method is little dangerous as you need to ensure all workers have the keytab file at the same location and you need
to remember this as you bring up new hosts in the cluster.

---

# HDFS Spout

Hdfs spout is intended to allow feeding data into Storm from a HDFS directory. 
It will actively monitor the directory to consume any new files that appear in the directory.
HDFS spout does not support Trident currently.

**Impt**: Hdfs spout assumes that the files being made visible to it in the monitored directory 
are NOT actively being written to. Only after a file is completely written should it be made
visible to the spout. This can be achieved by either writing the files out to another directory 
and once completely written, move it to the monitored directory. Alternatively the file
can be created with a '.ignore' suffix in the monitored directory and after data is completely 
written, rename it without the suffix. File names with a '.ignore' suffix are ignored
by the spout.

When the spout is actively consuming a file, it renames the file with a '.inprogress' suffix.
After consuming all the contents in the file, the file will be moved to a configurable *done* 
directory and the '.inprogress' suffix will be dropped.

**Concurrency** If multiple spout instances are used in the topology, each instance will consume
a different file. Synchronization among spout instances is done using lock files created in a 
(by default) '.lock' subdirectory under the monitored directory. A file with the same name
as the file being consumed (without the in progress suffix) is created in the lock directory.
Once the file is completely consumed, the corresponding lock file is deleted.

**Recovery from failure**
Periodically, the spout also records progress information wrt to how much of the file has been
consumed in the lock file. In case of an crash of the spout instance (or force kill of topology) 
another spout can take over the file and resume from the location recorded in the lock file.

Certain error conditions (such spout crashing) can leave behind lock files without deleting them. 
Such a stale lock file also indicates that the corresponding input file has also not been completely 
processed. When detected, ownership of such stale lock files will be transferred to another spout.   
The configuration 'hdfsspout.lock.timeout.sec' is used to specify the duration of inactivity after 
which lock files should be considered stale. For lock file ownership transfer to succeed, the HDFS
lease on the file (from prev lock owner) should have expired. Spouts scan for stale lock files
before selecting the next file for consumption.

**Lock on *.lock* Directory**
Hdfs spout instances create a *DIRLOCK* file in the .lock directory to co-ordinate certain accesses to 
the .lock dir itself. A spout will try to create it when it needs access to the .lock directory and
then delete it when done.  In error conditions such as a topology crash, force kill or untimely death 
of a spout, this file may not get deleted. Future running instances of the spout will eventually recover
this once the DIRLOCK file becomes stale due to inactivity for hdfsspout.lock.timeout.sec seconds.

## Usage

The following example creates an HDFS spout that reads text files from HDFS path hdfs://localhost:54310/source.

```java
// Instantiate spout
HdfsSpout textReaderSpout = new HdfsSpout().withOutputFields(TextFileReader.defaultFields);
// HdfsSpout seqFileReaderSpout = new HdfsSpout().withOutputFields(SequenceFileReader.defaultFields);

// textReaderSpout.withConfigKey("custom.keyname"); // Optional. Not required normally unless you need to change the keyname use to provide hds settings. This keyname defaults to 'hdfs.config' 

// Configure it
Config conf = new Config();
conf.put(Configs.SOURCE_DIR, "hdfs://localhost:54310/source");
conf.put(Configs.ARCHIVE_DIR, "hdfs://localhost:54310/done");
conf.put(Configs.BAD_DIR, "hdfs://localhost:54310/badfiles");
conf.put(Configs.READER_TYPE, "text"); // or 'seq' for sequence files

// Create & configure topology
TopologyBuilder builder = new TopologyBuilder();
builder.setSpout("hdfsspout", textReaderSpout, SPOUT_NUM);

// Setup bolts and other topology configuration
     ..snip..

// Submit topology with config
StormSubmitter.submitTopologyWithProgressBar("topologyName", conf, builder.createTopology());
```

See sample HdfsSpoutTopolgy in storm-starter.

## Configuration Settings
Class HdfsSpout provided following methods for configuration:

`HdfsSpout withOutputFields(String... fields)` : This sets the names for the output fields. 
The number of fields depends upon the reader being used. For convenience, built-in reader types 
expose a static member called `defaultFields` that can be used for this. 
 
 `HdfsSpout withConfigKey(String configKey)`
Optional setting. It allows overriding the default key name ('hdfs.config') with new name for 
specifying HDFS configs. Typically used to specify kerberos keytab and principal.

**E.g:**
```java
    HashMap map = new HashMap();
    map.put("hdfs.keytab.file", "/path/to/keytab");
    map.put("hdfs.kerberos.principal","user@EXAMPLE.com");
    conf.set("hdfs.config", map)
```

Only settings mentioned in **bold** are required.

| Setting                      | Default     | Description |
|------------------------------|-------------|-------------|
|**hdfsspout.reader.type**     |             | Indicates the reader for the file format. Set to 'seq' for reading sequence files or 'text' for text files. Set to a fully qualified class name if using a custom type (that implements interface org.apache.storm.hdfs.spout.FileReader)|
|**hdfsspout.hdfs**            |             | HDFS URI. Example:  hdfs://namenodehost:8020
|**hdfsspout.source.dir**      |             | HDFS location from where to read.  E.g. /data/inputfiles  |
|**hdfsspout.archive.dir**     |             | After a file is processed completely it will be moved to this directory. E.g. /data/done|
|**hdfsspout.badfiles.dir**    |             | if there is an error parsing a file's contents, the file is moved to this location.  E.g. /data/badfiles  |
|hdfsspout.lock.dir            | '.lock' subdirectory under hdfsspout.source.dir | Dir in which lock files will be created. Concurrent HDFS spout instances synchronize using *lock* files. Before processing a file the spout instance creates a lock file in this directory with same name as input file and deletes this lock file after processing the file. Spouts also periodically makes a note of their progress (wrt reading the input file) in the lock file so that another spout instance can resume progress on the same file if the spout dies for any reason.|
|hdfsspout.ignore.suffix       |   .ignore   | File names with this suffix in the in the hdfsspout.source.dir location will not be processed|
|hdfsspout.commit.count        |    20000    | Record progress in the lock file after these many records are processed. If set to 0, this criterion will not be used. |
|hdfsspout.commit.sec          |    10       | Record progress in the lock file after these many seconds have elapsed. Must be greater than 0 |
|hdfsspout.max.outstanding     |   10000     | Limits the number of unACKed tuples by pausing tuple generation (if ACKers are used in the topology) |
|hdfsspout.lock.timeout.sec    |  5 minutes  | Duration of inactivity after which a lock file is considered to be abandoned and ready for another spout to take ownership |
|hdfsspout.clocks.insync       |    true     | Indicates whether clocks on the storm machines are in sync (using services like NTP). Used for detecting stale locks. |
|hdfs.config (unless changed)  |             | Set it to a Map of Key/value pairs indicating the HDFS settigns to be used. For example, keytab and principle could be set using this. See section **Using keytabs on all worker hosts** under HDFS bolt below.| 

---

# License

Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.

# Committer Sponsors

 * P. Taylor Goetz ([ptgoetz@apache.org](mailto:ptgoetz@apache.org))
 * Bobby Evans ([bobby@apache.org](mailto:bobby@apache.org))
