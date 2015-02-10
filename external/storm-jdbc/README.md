#Storm JDBC
Storm/Trident integration for JDBC. This package includes the core bolts and trident states that allows a storm topology
to either insert storm tuples in a database table or to execute select queries against a database and enrich tuples 
in a storm topology. This code uses HikariCP for connection pooling. See http://brettwooldridge.github.io/HikariCP.

## Inserting into a database.
The bolt and trident state included in this package for inserting data into a database tables are tied to a single table.
The main API for inserting data in a table using JDBC is the `org.apache.storm.jdbc.mapper.JdbcMapper` interface:

```java
public interface JdbcMapper  extends Serializable {
    List<Column> getColumns(ITuple tuple);
}
```

The `getColumns()` method defines how a storm tuple maps to a list of columns representing a row in a database.

### SimpleJdbcMapper
`storm-jdbc` includes a general purpose `JdbcMapper` implementation called `SimpleJdbcMapper` that can map Storm
tuple to a Database row. `SimpleJdbcMapper` assumes that the storm tuple has fields with same name as the column name in 
the database table that you intend to write to.

To use `SimpleJdbcMapper`, you simply tell it the tableName that you want to write to and provide a hikari configuration map.

The following code creates a `SimpleJdbcMapper` instance that:

1. Will allow the mapper to transform a storm tuple to a list of columns mapping to a row in table test.user_details.
2. Will use the provided HikariCP configuration to establish a connection pool with specified Database configuration and
automatically figure out the column names and corresponding data types of the table that you intend to write to. 
Please see https://github.com/brettwooldridge/HikariCP#configuration-knobs-baby to learn more about hikari configuration properties.

```java
Map hikariConfigMap = Maps.newHashMap();
hikariConfigMap.put("dataSourceClassName","com.mysql.jdbc.jdbc2.optional.MysqlDataSource");
hikariConfigMap.put("dataSource.url", "jdbc:mysql://localhost/test");
hikariConfigMap.put("dataSource.user","root");
hikariConfigMap.put("dataSource.password","password");
String tableName = "user_details";
JdbcMapper simpleJdbcMapper = new SimpleJdbcMapper(tableName, map);
```
The mapper initialized in the example above assumes a storm tuple has value for all the columns. 
If your storm tuple only has fields for a subset of columns i.e. if some of the columns in your table have default values 
and you want to only insert values for columns with no default values you can enforce the behavior by initializing the 
`SimpleJdbcMapper` with explicit columnschema. For example, if you have a user_details table 
`create table if not exists user_details (user_id integer, user_name varchar(100), dept_name varchar(100), create_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP);`
In this table the create_time column has a default value. To ensure only the columns with no default values are inserted 
you can initialize the `jdbcMapper` as below:

```java
List<Column> columnSchema = Lists.newArrayList(
    new Column("user_id", java.sql.Types.INTEGER),
    new Column("user_name", java.sql.Types.VARCHAR));
    JdbcMapper simpleJdbcMapper = new SimpleJdbcMapper(columnSchema);
```

### JdbcInsertBolt
To use the `JdbcInsertBolt`, you construct an instance of it and specify a configuration key in your storm config that hold the 
hikari configuration map. In addition you must specify the JdbcMapper implementation to covert storm tuple to DB row and 
the table name in which the rows will be inserted. You can optionally specify a query timeout seconds param that specifies 
max seconds an insert query can take. The default is set to value of topology.message.timeout.secs.You should set this value 
to be <= topology.message.timeout.secs.

 ```java
Config config = new Config();
config.put("jdbc.conf", hikariConfigMap);
JdbcInsertBolt userPersistanceBolt = new JdbcInsertBolt("jdbc.conf","user_details",simpleJdbcMapper)
                                    .withQueryTimeoutSecs(30);
 ```
### JdbcTridentState
We also support a trident persistent state that can be used with trident topologies. To create a jdbc persistent trident
state you need to initialize it with the table name, the JdbcMapper instance and name of storm config key that holds the
hikari configuration map. See the example below:

```java
JdbcState.Options options = new JdbcState.Options()
        .withConfigKey("jdbc.conf")
        .withMapper(jdbcMapper)
        .withTableName("user_details")
        .withQueryTimeoutSecs(30);

JdbcStateFactory jdbcStateFactory = new JdbcStateFactory(options);
```

## Lookup from Database
We support `select` queries from databases to allow enrichment of storm tuples in a topology. The main API for 
executing select queries against a database using JDBC is the `org.apache.storm.jdbc.mapper.JdbcLookupMapper` interface:

```java
    void declareOutputFields(OutputFieldsDeclarer declarer);
    List<Column> getColumns(ITuple tuple);
    public List<Values> toTuple(ITuple input, List<Column> columns);
```

The `declareOutputFields` method is used to indicate what fields will be emitted as part of output tuple of processing a storm 
tuple. 
The `getColumns` method specifies the place holder columns in a select query and their SQL type and the value to use.
For example in the user_details table mentioned above if you were executing a query `select user_name from user_details where
user_id = ? and create_time > ?` the `getColumns` method would take a storm input tuple and return a List containing two items.
The first instance of `Column` type's `getValue()` method will be used as the value of `user_id` to lookup for and the
second instance of `Column` type's `getValue()` method will be used as the value of `create_time`.Note: the order in the
returned list determines the place holder's value. In other words the first item in the list maps to first `?` in select
query, the second item to second `?` in query and so on. 
The `toTuple` method takes in the input tuple and a list of columns representing a DB row as a result of the select query
and returns a list of values to be emitted. Please note that it returns a list of `Values` and not just a single instance
of `Values`. This allows a for a single DB row to be mapped to multiple output storm tuples.

###SimpleJdbcLookupMapper
`storm-jdbc` includes a general purpose `JdbcLookupMapper` implementation called `SimpleJdbcLookupMapper`. 

To use `SimpleJdbcMapper`, you have to initialize it with the fields that will be outputted by your bolt and the list of
columns that are used in your select query as place holder. The following example shows initialization of a `SimpleJdbcLookupMapper`
that declares `user_id,user_name,create_date` as output fields and `user_id` as the place holder column in select query.
SimpleJdbcMapper assumes the field name in your tuple is equal to the place holder column name, i.e. in our example 
`SimpleJdbcMapper` will look for a field `use_id` in the input tuple and use its value as the place holder's value in the
select query. For constructing output tuples, it looks for fields specified in `outputFields` in the input tuple first, 
and if it is not found in input tuple then it looks at select queries output row for a column with same name as field name. 
So in the example below if the input tuple had fields `user_id, create_date` and the select query was 
`select user_name from user_details where user_id = ?`, For each input tuple `SimpleJdbcLookupMapper.getColumns(tuple)` 
will return the value of `tuple.getValueByField("user_id")` which will be used as the value in `?` of select query. 
For each output row from DB, `SimpleJdbcLookupMapper.toTuple()` will use the `user_id, create_date` from the input tuple as 
is adding only `user_name` from the resulting row and returning these 3 fields as a single output tuple.

```java
Fields outputFields = new Fields("user_id", "user_name", "create_date");
List<Column> queryParamColumns = Lists.newArrayList(new Column("user_id", Types.INTEGER));
this.jdbcLookupMapper = new SimpleJdbcLookupMapper(outputFields, queryParamColumns);
```

### JdbcLookupBolt
To use the `JdbcLookupBolt`, construct an instance of it and specify a configuration key in your storm config that hold the 
hikari configuration map. In addition you must specify the `JdbcLookupMapper` and the select query to execute.
You can optionally specify a query timeout seconds param that specifies max seconds the select query can take. 
The default is set to value of topology.message.timeout.secs. You should set this value to be <= topology.message.timeout.secs.

```java
String selectSql = "select user_name from user_details where user_id = ?";
SimpleJdbcLookupMapper lookupMapper = new SimpleJdbcLookupMapper(outputFields, queryParamColumns)
JdbcLookupBolt userNameLookupBolt = new JdbcLookupBolt("jdbc.conf", selectSql, lookupMapper)
        .withQueryTimeoutSecs(30);
```

### JdbcTridentState for lookup
We also support a trident query state that can be used with trident topologies. 

```java
JdbcState.Options options = new JdbcState.Options()
        .withConfigKey("jdbc.conf")
        .withJdbcLookupMapper(new SimpleJdbcLookupMapper(new Fields("user_name"), Lists.newArrayList(new Column("user_id", Types.INTEGER))))
        .withSelectQuery("select user_name from user_details where user_id = ?");
        .withQueryTimeoutSecs(30);
```

## Example:
A runnable example can be found in the `src/test/java/topology` directory.

### Setup
* Ensure you have included JDBC implementation dependency for your chosen database as part of your build configuration.
* The test topologies executes the following queries so your intended DB must support these queries for test topologies
to work. 
```SQL
create table if not exists user (user_id integer, user_name varchar(100), dept_name varchar(100), create_date date);
create table if not exists department (dept_id integer, dept_name varchar(100));
create table if not exists user_department (user_id integer, dept_id integer);
insert into department values (1, 'R&D');
insert into department values (2, 'Finance');
insert into department values (3, 'HR');
insert into department values (4, 'Sales');
insert into user_department values (1, 1);
insert into user_department values (2, 2);
insert into user_department values (3, 3);
insert into user_department values (4, 4);
select dept_name from department, user_department where department.dept_id = user_department.dept_id and user_department.user_id = ?;
```
### Execution
Run the `org.apache.storm.jdbc.topology.UserPersistanceTopology` class using storm jar command. The class expects 5 args
storm jar org.apache.storm.jdbc.topology.UserPersistanceTopology <dataSourceClassName> <dataSource.url> <user> <password> [topology name]

To make it work with Mysql, you can add the following to the pom.xml

```
<dependency>
    <groupId>mysql</groupId>
    <artifactId>mysql-connector-java</artifactId>
    <version>5.1.31</version>
</dependency>
```

You can generate a single jar with dependencies using mvn assembly plugin. To use the plugin add the following to your pom.xml and execute 
mvn clean compile assembly:single.

```
<plugin>
    <artifactId>maven-assembly-plugin</artifactId>
    <configuration>
        <archive>
            <manifest>
                <mainClass>fully.qualified.MainClass</mainClass>
            </manifest>
        </archive>
        <descriptorRefs>
            <descriptorRef>jar-with-dependencies</descriptorRef>
        </descriptorRefs>
    </configuration>
</plugin>
```

Mysql Example:
```
storm jar ~/repo/incubator-storm/external/storm-jdbc/target/storm-jdbc-0.10.0-SNAPSHOT-jar-with-dependencies.jar org.apache.storm.jdbc.topology.UserPersistanceTopology  com.mysql.jdbc.jdbc2.optional.MysqlDataSource jdbc:mysql://localhost/test root password UserPersistenceTopology
```

You can execute a select query against the user table which should show newly inserted rows:

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

## Committer Sponsors
 * P. Taylor Goetz ([ptgoetz@apache.org](mailto:ptgoetz@apache.org))
 * Sriharsha Chintalapani ([sriharsha@apache.org](mailto:sriharsha@apache.org)) 