---
title: Storm JDBC Integration
layout: documentation
documentation: true
---

Storm/Trident integration for JDBC. This package includes the core bolts and trident states that allows a storm topology
to either insert storm tuples in a database table or to execute select queries against a database and enrich tuples 
in a storm topology.

**Note**: Throughout the examples below, we make use of com.google.common.collect.Lists and com.google.common.collect.Maps.

## Inserting into a database.
The bolt and trident state included in this package for inserting data into a database tables are tied to a single table.

### ConnectionProvider
An interface that should be implemented by different connection pooling mechanism `org.apache.storm.jdbc.common.ConnectionProvider`

```java
public interface ConnectionProvider extends Serializable {
    /**
     * method must be idempotent.
     */
    void prepare();

    /**
     *
     * @return a DB connection over which the queries can be executed.
     */
    Connection getConnection();

    /**
     * called once when the system is shutting down, should be idempotent.
     */
    void cleanup();
}
```

Out of the box we support `org.apache.storm.jdbc.common.HikariCPConnectionProvider` which is an implementation that uses HikariCP.

###JdbcMapper
The main API for inserting data in a table using JDBC is the `org.apache.storm.jdbc.mapper.JdbcMapper` interface:

```java
public interface JdbcMapper  extends Serializable {
    List<Column> getColumns(ITuple tuple);
}
```

The `getColumns()` method defines how a storm tuple maps to a list of columns representing a row in a database. 
**The order of the returned list is important. The place holders in the supplied queries are resolved in the same order as returned list.**
For example if the user supplied insert query is `insert into user(user_id, user_name, create_date) values (?,?, now())` the 1st item 
of the returned list of `getColumns` method will map to the 1st place holder and the 2nd to the 2nd and so on. We do not parse
the supplied queries to try and resolve place holder by column names. Not making any assumptions about the query syntax allows this connector
to be used by some non-standard sql frameworks like Pheonix which only supports upsert into.

### JdbcInsertBolt
To use the `JdbcInsertBolt`, you construct an instance of it by specifying a `ConnectionProvider` implementation
and a `JdbcMapper` implementation that converts storm tuple to DB row. In addition, you must either supply
a table name  using `withTableName` method or an insert query using `withInsertQuery`. 
If you specify a insert query you should ensure that your `JdbcMapper` implementation will return a list of columns in the same order as in your insert query.
You can optionally specify a query timeout seconds param that specifies max seconds an insert query can take. 
The default is set to value of topology.message.timeout.secs and a value of -1 will indicate not to set any query timeout.
You should set the query timeout value to be <= topology.message.timeout.secs.

 ```java
Map hikariConfigMap = Maps.newHashMap();
hikariConfigMap.put("dataSourceClassName","com.mysql.jdbc.jdbc2.optional.MysqlDataSource");
hikariConfigMap.put("dataSource.url", "jdbc:mysql://localhost/test");
hikariConfigMap.put("dataSource.user","root");
hikariConfigMap.put("dataSource.password","password");
ConnectionProvider connectionProvider = new HikariCPConnectionProvider(hikariConfigMap);

String tableName = "user_details";
JdbcMapper simpleJdbcMapper = new SimpleJdbcMapper(tableName, connectionProvider);

JdbcInsertBolt userPersistanceBolt = new JdbcInsertBolt(connectionProvider, simpleJdbcMapper)
                                    .withTableName("user")
                                    .withQueryTimeoutSecs(30);
                                    Or
JdbcInsertBolt userPersistanceBolt = new JdbcInsertBolt(connectionProvider, simpleJdbcMapper)
                                    .withInsertQuery("insert into user values (?,?)")
                                    .withQueryTimeoutSecs(30);                                    
 ```

### SimpleJdbcMapper
`storm-jdbc` includes a general purpose `JdbcMapper` implementation called `SimpleJdbcMapper` that can map Storm
tuple to a Database row. `SimpleJdbcMapper` assumes that the storm tuple has fields with same name as the column name in 
the database table that you intend to write to.

To use `SimpleJdbcMapper`, you simply tell it the tableName that you want to write to and provide a connectionProvider instance.

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
ConnectionProvider connectionProvider = new HikariCPConnectionProvider(hikariConfigMap);
String tableName = "user_details";
JdbcMapper simpleJdbcMapper = new SimpleJdbcMapper(tableName, connectionProvider);
```
The mapper initialized in the example above assumes a storm tuple has value for all the columns of the table you intend to insert data into and its `getColumn`
method will return the columns in the order in which Jdbc connection instance's `connection.getMetaData().getColumns();` method returns them.

**If you specified your own insert query to `JdbcInsertBolt` you must initialize `SimpleJdbcMapper` with explicit columnschema such that the schema has columns in the same order as your insert queries.**
For example if your insert query is `Insert into user (user_id, user_name) values (?,?)` then your `SimpleJdbcMapper` should be initialized with the following statements:
```java
List<Column> columnSchema = Lists.newArrayList(
    new Column("user_id", java.sql.Types.INTEGER),
    new Column("user_name", java.sql.Types.VARCHAR));
JdbcMapper simpleJdbcMapper = new SimpleJdbcMapper(columnSchema);
```

If your storm tuple only has fields for a subset of columns i.e. if some of the columns in your table have default values and you want to only insert values for columns with no default values you can enforce the behavior by initializing the 
`SimpleJdbcMapper` with explicit columnschema. For example, if you have a user_details table `create table if not exists user_details (user_id integer, user_name varchar(100), dept_name varchar(100), create_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP);`
In this table the create_time column has a default value. To ensure only the columns with no default values are inserted 
you can initialize the `jdbcMapper` as below:

```java
List<Column> columnSchema = Lists.newArrayList(
    new Column("user_id", java.sql.Types.INTEGER),
    new Column("user_name", java.sql.Types.VARCHAR),
    new Column("dept_name", java.sql.Types.VARCHAR));
JdbcMapper simpleJdbcMapper = new SimpleJdbcMapper(columnSchema);
```
### JdbcTridentState
We also support a trident persistent state that can be used with trident topologies. To create a jdbc persistent trident
state you need to initialize it with the table name or an insert query, the JdbcMapper instance and connection provider instance.
See the example below:

```java
JdbcState.Options options = new JdbcState.Options()
        .withConnectionProvider(connectionProvider)
        .withMapper(jdbcMapper)
        .withTableName("user_details")
        .withQueryTimeoutSecs(30);
JdbcStateFactory jdbcStateFactory = new JdbcStateFactory(options);
```
similar to `JdbcInsertBolt` you can specify a custom insert query using `withInsertQuery` instead of specifying a table name.

## Lookup from Database
We support `select` queries from databases to allow enrichment of storm tuples in a topology. The main API for 
executing select queries against a database using JDBC is the `org.apache.storm.jdbc.mapper.JdbcLookupMapper` interface:

```java
    void declareOutputFields(OutputFieldsDeclarer declarer);
    List<Column> getColumns(ITuple tuple);
    List<Values> toTuple(ITuple input, List<Column> columns);
```

The `declareOutputFields` method is used to indicate what fields will be emitted as part of output tuple of processing a storm 
tuple. 

The `getColumns` method specifies the place holder columns in a select query and their SQL type and the value to use.
For example in the user_details table mentioned above if you were executing a query `select user_name from user_details where
user_id = ? and create_time > ?` the `getColumns` method would take a storm input tuple and return a List containing two items.
The first instance of `Column` type's `getValue()` method will be used as the value of `user_id` to lookup for and the
second instance of `Column` type's `getValue()` method will be used as the value of `create_time`.
**Note: the order in the returned list determines the place holder's value. In other words the first item in the list maps 
to first `?` in select query, the second item to second `?` in query and so on.** 

The `toTuple` method takes in the input tuple and a list of columns representing a DB row as a result of the select query
and returns a list of values to be emitted. 
**Please note that it returns a list of `Values` and not just a single instance of `Values`.** 
This allows a for a single DB row to be mapped to multiple output storm tuples.

###SimpleJdbcLookupMapper
`storm-jdbc` includes a general purpose `JdbcLookupMapper` implementation called `SimpleJdbcLookupMapper`. 

To use `SimpleJdbcMapper`, you have to initialize it with the fields that will be outputted by your bolt and the list of
columns that are used in your select query as place holder. The following example shows initialization of a `SimpleJdbcLookupMapper`
that declares `user_id,user_name,create_date` as output fields and `user_id` as the place holder column in select query.
SimpleJdbcMapper assumes the field name in your tuple is equal to the place holder column name, i.e. in our example 
`SimpleJdbcMapper` will look for a field `use_id` in the input tuple and use its value as the place holder's value in the
select query. For constructing output tuples, it looks for fields specified in `outputFields` in the input tuple first, 
and if it is not found in input tuple then it looks at select query's output row for a column with same name as field name. 
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
To use the `JdbcLookupBolt`, construct an instance of it using a `ConnectionProvider` instance, `JdbcLookupMapper` instance and the select query to execute.
You can optionally specify a query timeout seconds param that specifies max seconds the select query can take. 
The default is set to value of topology.message.timeout.secs. You should set this value to be <= topology.message.timeout.secs.

```java
String selectSql = "select user_name from user_details where user_id = ?";
SimpleJdbcLookupMapper lookupMapper = new SimpleJdbcLookupMapper(outputFields, queryParamColumns)
JdbcLookupBolt userNameLookupBolt = new JdbcLookupBolt(connectionProvider, selectSql, lookupMapper)
        .withQueryTimeoutSecs(30);
```

### JdbcTridentState for lookup
We also support a trident query state that can be used with trident topologies. 

```java
JdbcState.Options options = new JdbcState.Options()
        .withConnectionProvider(connectionProvider)
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
`mvn clean compile assembly:single`

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
