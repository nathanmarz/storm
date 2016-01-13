# Storm SQL

Compile SQL queries to Storm topologies.

## Usage

Run the ``storm sql`` command to compile SQL statements into Trident topology, and submit it to the Storm cluster

```
$ bin/storm sql <sql-file> <topo-name>
```

In which `sql-file` contains a list of SQL statements to be executed, and `topo-name` is the name of the topology.

## Supported Features

The following features are supported in the current repository:

* Streaming from and to external data sources
* Filtering tuples
* Projections

## Specifying External Data Sources

In StormSQL data is represented by external tables. Users can specify data sources using the `CREATE EXTERNAL TABLE`
statement. For example, the following statement specifies a Kafka spouts and sink:

```
CREATE EXTERNAL TABLE FOO (ID INT PRIMARY KEY) LOCATION 'kafka://localhost:2181/brokers?topic=test' TBLPROPERTIES '{"producer":{"bootstrap.servers":"localhost:9092","acks":"1","key.serializer":"org.apache.storm.kafka.IntSerializer","value.serializer":"org.apache.storm.kafka.ByteBufferSerializer"}}'
```

The syntax of `CREATE EXTERNAL TABLE` closely follows the one defined in
[Hive Data Definition Language](https://cwiki.apache.org/confluence/display/Hive/LanguageManual+DDL).

## Plugging in External Data Sources

Users plug in external data sources through implementing the `ISqlTridentDataSource` interface and registers them using
the mechanisms of Java's service loader. The external data source will be chosen based on the scheme of the URI of the
tables. Please refer to the implementation of `storm-sql-kafka` for more details.

## Example: Filtering Kafka Stream

Let's say there is a Kafka stream that represents the transactions of orders. Each message in the stream contains the id
of the order, the unit price of the product and the quantity of the orders. The goal is to filter orders where the
transactions are significant and to insert these orders into another Kafka stream for further analysis.

The user can specify the following SQL statements in the SQL file:

```
CREATE EXTERNAL TABLE ORDERS (ID INT PRIMARY KEY, UNIT_PRICE INT, QUANTITY INT) LOCATION 'kafka://localhost:2181/brokers?topic=orders' TBLPROPERTIES '{"producer":{"bootstrap.servers":"localhost:9092","acks":"1","key.serializer":"org.apache.storm.kafka.IntSerializer","value.serializer":"org.apache.storm.kafka.ByteBufferSerializer"}}'

CREATE EXTERNAL TABLE LARGE_ORDERS (ID INT PRIMARY KEY, TOTAL INT) LOCATION 'kafka://localhost:2181/brokers?topic=large_orders' TBLPROPERTIES '{"producer":{"bootstrap.servers":"localhost:9092","acks":"1","key.serializer":"org.apache.storm.kafka.IntSerializer","value.serializer":"org.apache.storm.kafka.ByteBufferSerializer"}}'

INSERT INTO LARGE_ORDERS SELECT ID, UNIT_PRICE * QUANTITY AS TOTAL FROM ORDERS WHERE UNIT_PRICE * QUANTITY > 50
```

The first statement defines the table `ORDER` which represents the input stream. The `LOCATION` clause specifies the
ZkHost (`localhost:2181`), the path of the brokers in ZooKeeper (`/brokers`) and the topic (`orders`).
The `TBLPROPERTIES` clause specifies the configuration of
[KafkaProducer](http://kafka.apache.org/documentation.html#newproducerconfigs).
Current implementation of `storm-sql-kafka` requires specifying both `LOCATION` and `TBLPROPERTIES` clauses even though
the table is read-only or write-only.

Similarly, the second statement specifies the table `LARGE_ORDERS` which represents the output stream. The third
statement is a `SELECT` statement which defines the topology: it instructs StormSQL to filter all orders in the external
table `ORDERS`, calculates the total price and inserts matching records into the Kafka stream specified by
`LARGE_ORDER`.

To run this example, users need to include the data sources (`storm-sql-kafka` in this case) and its dependency in the
class path. One approach is to put the required jars into the `extlib` directory:

```
$ cp curator-client-2.5.0.jar curator-framework-2.5.0.jar zookeeper-3.4.6.jar
 extlib/
$ cp scala-library-2.10.4.jar kafka-clients-0.8.2.1.jar kafka_2.10-0.8.2.1.jar metrics-core-2.2.0.jar extlib/
$ cp json-simple-1.1.1.jar extlib/
$ cp storm-kafka-*.jar storm-sql-kafka-*.jar storm-sql-runtime-*.jar extlib/
```

The next step is to submit the SQL statements to StormSQL:

```
$ bin/storm sql order_filtering order_filtering.sql
```

By now you should be able to see the `order_filtering` topology in the Storm UI.

## Current Limitations

Aggregation, windowing and joining tables are yet to be implemented. Specifying parallelism hints in the topology is not
yet supported.

Users also need to provide the dependency of the external data sources in the `extlib` directory. Otherwise the topology
will fail to run because of `ClassNotFoundException`.

## License

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

## Committer Sponsors
 * Sriharsha Chintalapani ([sriharsha@apache.org](mailto:sriharsha@apache.org))
 * P. Taylor Goetz ([ptgoetz@apache.org](mailto:ptgoetz@apache.org))
