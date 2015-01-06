#Storm HBase

Storm/Trident integration for JDBC.

## Usage
The main API for interacting with JDBC is the `org.apache.storm.jdbc.mapper.TupleToColumnMapper`
interface:

```java
public interface JdbcMapper  extends Serializable {
    List<Column> getColumns(ITuple tuple);
}
```

The `getColumns()` method defines how a storm tuple maps to a list of columns representing a row in a database.

### SimpleJdbcMapper
`storm-jdbc` includes a general purpose `JdbcMapper` implementation called `SimpleJdbcMapper` that can map Storm
tuple to a Database row. `SimpleJdbcMapper` assumes that the tuple has fields with same name as the column name in 
the database table that you intend to write to.

To use `SimpleJdbcMapper`, you simply tell it the tableName that you want to write to and provide a hikari configuration map.

The following code creates a `SimpleJdbcMapper` instance that:

1. Will allow the mapper to transform a storm tuple to a list of columns mapping to a row in table test.user_details.
2. Will use the provided HikariCP configuration to establish a connection pool with specified Database configuration and
automatically figure out the column names of the table that you intend to write to. 
Please see https://github.com/brettwooldridge/HikariCP#configuration-knobs-baby to lear more about hikari configuration properties.

```java
Map hikariConfigMap = Maps.newHashMap();
hikariConfigMap.put("dataSourceClassName","com.mysql.jdbc.jdbc2.optional.MysqlDataSource");
hikariConfigMap.put("dataSource.url", "jdbc:mysql://localhost/test");
hikariConfigMap.put("dataSource.user","root");
hikariConfigMap.put("dataSource.password","password");
String tableName = "user_details";
JdbcMapper jdbcMapper = new SimpleJdbcMapper(tableName, map);
```
### JdbcBolt
To use the `JdbcBolt`, construct it with the name of the table to write to, and a `JdbcMapper` implementation. In addition
you must specify a configuration key that hold the hikari configuration map.

 ```java
Config config = new Config();
config.put("jdbc.conf", hikariConfigMap);

JdbcBolt bolt = new JdbcBolt("user_details", jdbcMapper)
        .withConfigKey("jdbc.conf");
 ```
### JdbcTridentState
We also support a trident persistent state that can be used with trident topologies. To create a jdbc persistent trident
state you need to initialize it with the table name, the JdbcMapper instance and hikari configuration. See the example
below:

```java
JdbcState.Options options = new JdbcState.Options()
        .withConfigKey("jdbc.conf")
        .withMapper(jdbcMapper)
        .withTableName("user");

JdbcStateFactory jdbcStateFactory = new JdbcStateFactory(options);
```
 
## Example: Persistent User details
A runnable example can be found in the `src/test/java/topology` directory.

### Setup
* Ensure you have included JDBC implementation dependency for your chosen database as part of your build configuration.
* Start the database and login to the database.
* Create table user using the following query:

```
> use test;
> create table user (id integer, user_name varchar(100), create_date date);
```

### Execution
Run the `org.apache.storm.jdbc.topology.UserPersistanceTopology` class using storm jar command. The class expects 5 args
storm jar org.apache.storm.jdbc.topology.UserPersistanceTopology <dataSourceClassName> <dataSource.url> <user> <password> <tableName> [topology name]

Mysql Example:
```
storm jar ~/repo/incubator-storm/external/storm-jdbc/target/storm-jdbc-0.10.0-SNAPSHOT-jar-with-dependencies.jar 
org.apache.storm.jdbc.topology.UserPersistanceTridentTopology  com.mysql.jdbc.jdbc2.optional.MysqlDataSource 
jdbc:mysql://localhost/test root password user UserPersistenceTopology
```

You can execute a select query against the user table which shoule show newly inserted rows:

```
select * from user;
```

For trident you can view `org.apache.storm.jdbc.topology.UserPersistanceTridentTopology`.
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

## Committer

* Parth Brahmbhatt ([brahmbhatt.parth@gmail.com](mailto:brahmbhatt.parth@gmail.com))
 