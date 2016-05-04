---
title: Storm Cassandra Integration
layout: documentation
documentation: true
---

### Bolt API implementation for Apache Cassandra

This library provides core storm bolt on top of Apache Cassandra.
Provides simple DSL to map storm *Tuple* to Cassandra Query Language *Statement*.


### Configuration
The following properties may be passed to storm configuration.

| **Property name**                            | **Description** | **Default**         |
| ---------------------------------------------| ----------------| --------------------|
| **cassandra.keyspace**                       | -               |                     |
| **cassandra.nodes**                          | -               | {"localhost"}       |
| **cassandra.username**                       | -               | -                   |
| **cassandra.password**                       | -               | -                   |
| **cassandra.port**                           | -               | 9092                |
| **cassandra.output.consistencyLevel**        | -               | ONE                 |
| **cassandra.batch.size.rows**                | -               | 100                 |
| **cassandra.retryPolicy**                    | -               | DefaultRetryPolicy  |
| **cassandra.reconnectionPolicy.baseDelayMs** | -               | 100 (ms)            |
| **cassandra.reconnectionPolicy.maxDelayMs**  | -               | 60000 (ms)          |

### CassandraWriterBolt

####Static import
```java

import static org.apache.storm.cassandra.DynamicStatementBuilder.*

```

#### Insert Query Builder
##### Insert query including only the specified tuple fields.
```java

    new CassandraWriterBolt(
        async(
            simpleQuery("INSERT INTO album (title,year,performer,genre,tracks) VALUES (?, ?, ?, ?, ?);")
                .with(
                    fields("title", "year", "performer", "genre", "tracks")
                 )
            )
    );
```

##### Insert query including all tuple fields.
```java

    new CassandraWriterBolt(
        async(
            simpleQuery("INSERT INTO album (title,year,performer,genre,tracks) VALUES (?, ?, ?, ?, ?);")
                .with( all() )
            )
    );
```

##### Insert multiple queries from one input tuple.
```java

    new CassandraWriterBolt(
        async(
            simpleQuery("INSERT INTO titles_per_album (title,year,performer,genre,tracks) VALUES (?, ?, ?, ?, ?);").with(all())),
            simpleQuery("INSERT INTO titles_per_performer (title,year,performer,genre,tracks) VALUES (?, ?, ?, ?, ?);").with(all()))
        )
    );
```

##### Insert query using QueryBuilder
```java

    new CassandraWriterBolt(
        async(
            simpleQuery("INSERT INTO album (title,year,perfomer,genre,tracks) VALUES (?, ?, ?, ?, ?);")
                .with(all()))
            )
    )
```

##### Insert query with static bound query
```java

    new CassandraWriterBolt(
         async(
            boundQuery("INSERT INTO album (title,year,performer,genre,tracks) VALUES (?, ?, ?, ?, ?);")
                .bind(all());
         )
    );
```

##### Insert query with static bound query using named setters and aliases
```java

    new CassandraWriterBolt(
         async(
            boundQuery("INSERT INTO album (title,year,performer,genre,tracks) VALUES (:ti, :ye, :pe, :ge, :tr);")
                .bind(
                    field("ti"),as("title"),
                    field("ye").as("year")),
                    field("pe").as("performer")),
                    field("ge").as("genre")),
                    field("tr").as("tracks"))
                ).byNamedSetters()
         )
    );
```

##### Insert query with bound statement load from storm configuration
```java

    new CassandraWriterBolt(
         boundQuery(named("insertIntoAlbum"))
            .bind(all());
```

##### Insert query with bound statement load from tuple field
```java

    new CassandraWriterBolt(
         boundQuery(namedByField("cql"))
            .bind(all());
```

##### Insert query with batch statement
```java

    // Logged
    new CassandraWriterBolt(loggedBatch(
            simpleQuery("INSERT INTO titles_per_album (title,year,performer,genre,tracks) VALUES (?, ?, ?, ?, ?);").with(all())),
            simpleQuery("INSERT INTO titles_per_performer (title,year,performer,genre,tracks) VALUES (?, ?, ?, ?, ?);").with(all()))
        )
    );
// UnLogged
    new CassandraWriterBolt(unLoggedBatch(
            simpleQuery("INSERT INTO titles_per_album (title,year,performer,genre,tracks) VALUES (?, ?, ?, ?, ?);").with(all())),
            simpleQuery("INSERT INTO titles_per_performer (title,year,performer,genre,tracks) VALUES (?, ?, ?, ?, ?);").with(all()))
        )
    );
```

### How to handle query execution results

The interface *ExecutionResultHandler* can be used to custom how an execution result should be handle.

```java
public interface ExecutionResultHandler extends Serializable {
    void onQueryValidationException(QueryValidationException e, OutputCollector collector, Tuple tuple);

    void onReadTimeoutException(ReadTimeoutException e, OutputCollector collector, Tuple tuple);

    void onWriteTimeoutException(WriteTimeoutException e, OutputCollector collector, Tuple tuple);

    void onUnavailableException(UnavailableException e, OutputCollector collector, Tuple tuple);

    void onQuerySuccess(OutputCollector collector, Tuple tuple);
}
```

By default, the CassandraBolt fails a tuple on all Cassandra Exception (see [BaseExecutionResultHandler](https://github.com/apache/storm/tree/master/external/storm-cassandra/blob/master/src/main/java/org/apache/storm/cassandra/BaseExecutionResultHandler.java)) .

```java
    new CassandraWriterBolt(insertInto("album").values(with(all()).build())
            .withResultHandler(new MyCustomResultHandler());
```

### Declare Output fields

A CassandraBolt can declare output fields / stream output fields.
For instance, this may be used to remit a new tuple on error, or to chain queries.

```java
    new CassandraWriterBolt(insertInto("album").values(withFields(all()).build())
            .withResultHandler(new EmitOnDriverExceptionResultHandler());
            .withStreamOutputFields("stream_error", new Fields("message");

    public static class EmitOnDriverExceptionResultHandler extends BaseExecutionResultHandler {
        @Override
        protected void onDriverException(DriverException e, OutputCollector collector, Tuple tuple) {
            LOG.error("An error occurred while executing cassandra statement", e);
            collector.emit("stream_error", new Values(e.getMessage()));
            collector.ack(tuple);
        }
    }
```

### Murmur3FieldGrouping

[Murmur3StreamGrouping](https://github.com/apache/storm/tree/master/external/storm-cassandra/blob/master/src/main/java/org/apache/storm/cassandra/Murmur3StreamGrouping.java)  can be used to optimise cassandra writes.
The stream is partitioned among the bolt's tasks based on the specified row partition keys.

```java
CassandraWriterBolt bolt = new CassandraWriterBolt(
    insertInto("album")
        .values(
            with(fields("title", "year", "performer", "genre", "tracks")
            ).build());
builder.setBolt("BOLT_WRITER", bolt, 4)
        .customGrouping("spout", new Murmur3StreamGrouping("title"))
```

### Trident API support
storm-cassandra support Trident `state` API for `inserting` data into Cassandra. 
```java
        CassandraState.Options options = new CassandraState.Options(new CassandraContext());
        CQLStatementTupleMapper insertTemperatureValues = boundQuery(
                "INSERT INTO weather.temperature(weather_station_id, weather_station_name, event_time, temperature) VALUES(?, ?, ?, ?)")
                .bind(with(field("weather_station_id"), field("name").as("weather_station_name"), field("event_time").now(), field("temperature")));
        options.withCQLStatementTupleMapper(insertTemperatureValues);
        CassandraStateFactory insertValuesStateFactory =  new CassandraStateFactory(options);
        TridentState selectState = topology.newStaticState(selectWeatherStationStateFactory);
        stream = stream.stateQuery(selectState, new Fields("weather_station_id"), new CassandraQuery(), new Fields("name"));
        stream = stream.each(new Fields("name"), new PrintFunction(), new Fields("name_x"));
        stream.partitionPersist(insertValuesStateFactory, new Fields("weather_station_id", "name", "event_time", "temperature"), new CassandraStateUpdater(), new Fields());
```

Below `state` API for `querying` data from Cassandra.
```java
        CassandraState.Options options = new CassandraState.Options(new CassandraContext());
        CQLStatementTupleMapper insertTemperatureValues = boundQuery("SELECT name FROM weather.station WHERE id = ?")
                 .bind(with(field("weather_station_id").as("id")));
        options.withCQLStatementTupleMapper(insertTemperatureValues);
        options.withCQLResultSetValuesMapper(new TridentResultSetValuesMapper(new Fields("name")));
        CassandraStateFactory selectWeatherStationStateFactory =  new CassandraStateFactory(options);
        CassandraStateFactory selectWeatherStationStateFactory = getSelectWeatherStationStateFactory();
        TridentState selectState = topology.newStaticState(selectWeatherStationStateFactory);
        stream = stream.stateQuery(selectState, new Fields("weather_station_id"), new CassandraQuery(), new Fields("name"));         
```

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
 * Sriharha Chintalapani ([sriharsha@apache.org](mailto:sriharsha@apache.org))
 * P. Taylor Goetz ([ptgoetz@apache.org](mailto:ptgoetz@apache.org))
