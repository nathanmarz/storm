---
title: Storm Solr Integration
layout: documentation
documentation: true
---

Storm and Trident integration for Apache Solr. This package includes a bolt and a trident state that enable a Storm topology
stream the contents of storm tuples to index Solr collections.
 
# Index Storm tuples into a Solr collection
The The bolt and trident state provided use one of the supplied mappers to build a `SolrRequest` object that is 
responsible for making the update calls to Solr, thus updating the index of the collection specified.
 
# Usage Examples 
In this section we provide some simple code snippets on how to build Storm and Trident topologies to index Solr. In subsequent sections we 
describe in detail the two key components of the Storm Solr integration, the `SolrUpdateBolt`, and the `Mappers`, `SolrFieldsMapper`, and `SolrJsonMapper`.

## Storm Bolt With JSON Mapper and Count Based Commit Strategy

```java
    new SolrUpdateBolt(solrConfig, solrMapper, solrCommitStgy)
    
    // zkHostString for Solr 'gettingstarted' example
    SolrConfig solrConfig = new SolrConfig("127.0.0.1:9983");
    
    // JSON Mapper used to generate 'SolrRequest' requests to update the "gettingstarted" Solr collection with JSON content declared the tuple field with name "JSON"
    SolrMapper solrMapper = new SolrJsonMapper.Builder("gettingstarted", "JSON").build(); 
     
    // Acks every other five tuples. Setting to null acks every tuple
    SolrCommitStrategy solrCommitStgy = new CountBasedCommit(5);          
```

## Trident Topology With Fields Mapper
```java
    new SolrStateFactory(solrConfig, solrMapper);
    
    // zkHostString for Solr 'gettingstarted' example
    SolrConfig solrConfig = new SolrConfig("127.0.0.1:9983");
    
    /* Solr Fields Mapper used to generate 'SolrRequest' requests to update the "gettingstarted" Solr collection. The Solr index is updated using the field values of the tuple fields that match static or dynamic fields declared in the schema object build using schemaBuilder */ 
    SolrMapper solrMapper = new SolrFieldsMapper.Builder(schemaBuilder, "gettingstarted").build();

    // builds the Schema object from the JSON representation of the schema as returned by the URL http://localhost:8983/solr/gettingstarted/schema/ 
    SchemaBuilder schemaBuilder = new RestJsonSchemaBuilder("localhost", "8983", "gettingstarted")
```

## SolrUpdateBolt
 `SolrUpdateBolt` streams tuples directly into Apache Solr. The Solr index is updated using`SolrRequest` requests. 
 The `SolrUpdateBolt` is configurable using implementations of `SolrConfig`, `SolrMapper`, and optionally `SolrCommitStrategy`.
   
 The data to stream onto Solr is extracted from the tuples using the strategy defined in the `SolrMapper` implementation.
 
 The `SolrRquest` can be sent every tuple, or according to a strategy defined by `SolrCommitStrategy` implementations. 
 If a `SolrCommitStrategy` is in place and one of the tuples in the batch fails, the batch is not committed, and all the tuples in that 
  batch are marked as Fail, and retried. On the other hand, if all tuples succeed, the `SolrRequest` is committed and all tuples are successfully acked.
 
 `SolrConfig` is the class containing Solr configuration to be made available to Storm Solr bolts. Any configuration needed in the bolts should be put in this class.
 

## SolrMapper
`SorlMapper` implementations define the strategy to extract information from the tuples. The public method
`toSolrRequest` receives a tuple or a list of tuples and returns a `SolrRequest` object that is used to update the Solr index.


### SolrJsonMapper
The `SolrJsonMapper` creates a Solr update request that is sent to the URL endpoint defined by Solr as the resource 
destination for requests in JSON format. 
 
To create a `SolrJsonMapper` the client must specify the name of the collection to update as well as the 
tuple field that contains the JSON object used to update the Solr index. If the tuple does not contain the field specified, 
a `SolrMapperException` will be thrown when the method `toSolrRequest`is called. If the field exists, its value can either 
be a String with the contents in JSON format, or a Java object that will be serialized to JSON
 
Code snippet illustrating how to create a `SolrJsonMapper` object to update the `gettingstarted` Solr collection with JSON content 
declared in the tuple field with name "JSON"
``` java
    SolrMapper solrMapper = new SolrJsonMapper.Builder("gettingstarted", "JSON").build();
```


### SolrFieldsMapper
The `SolrFieldsMapper` creates a Solr update request that is sent to the Solr URL endpoint that handles the updates of `SolrInputDocument` objects.

To create a `SolrFieldsMapper` the client must specify the name of the collection to update as well as the `SolrSchemaBuilder`. 
The Solr `Schema` is used to extract information about the Solr schema fields and corresponding types. This metadata is used
to get the information from the tuples. Only tuple fields that match a static or dynamic Solr fields are added to the document. Tuple fields 
that do not match the schema are not added to the `SolrInputDocument` being prepared for indexing. A debug log message is printed for the 
 tuple fields that do not match the schema and hence are not indexed.
 

The `SolrFieldsMapper` supports multivalue fields. A multivalue tuple field must be tokenized. The default token is |. Any 
arbitrary token can be specified by calling the method `org.apache.storm.solr.mapper.SolrFieldsMapper.Builder.setMultiValueFieldToken`
that is part of the `SolrFieldsMapper.Builder` builder class. 

Code snippet illustrating how to create a `SolrFieldsMapper` object to update the `gettingstarted` Solr collection. The multivalue 
field separates each value with the token % instead of the default | . To use the default token you can ommit the call to the method
`setMultiValueFieldToken`.

``` java
    new SolrFieldsMapper.Builder(
            new RestJsonSchemaBuilder("localhost", "8983", "gettingstarted"), "gettingstarted")
                .setMultiValueFieldToken("%").build();
```

# Build And Run Bundled Examples  
To be able to run the examples you must first build the java code in the package `storm-solr`, 
and then generate an uber jar with all the dependencies.


## Build the Storm Apache Solr Integration Code

`mvn clean install -f REPO_HOME/storm/external/storm-solr/pom.xml`
 
## Use the Maven Shade Plugin to Build the Uber Jar

 Add the following to `REPO_HOME/storm/external/storm-solr/pom.xml`
 
 ```
 <plugin>
     <groupId>org.apache.maven.plugins</groupId>
     <artifactId>maven-shade-plugin</artifactId>
     <version>2.4.1</version>
     <executions>
         <execution>
             <phase>package</phase>
             <goals>
                 <goal>shade</goal>
             </goals>
             <configuration>
                 <transformers>
                     <transformer implementation="org.apache.maven.plugins.shade.resource.ManifestResourceTransformer">
                         <mainClass>org.apache.storm.solr.topology.SolrJsonTopology</mainClass>
                     </transformer>
                 </transformers>
             </configuration>
         </execution>
     </executions>
</plugin>
 ```

create the uber jar by running the commmand:

`mvn package -f REPO_HOME/storm/external/storm-solr/pom.xml`

This will create the uber jar file with the name and location matching the following pattern:
 
`REPO_HOME/storm/external/storm/target/storm-solr-0.11.0-SNAPSHOT.jar`

## Run Examples
Copy the file `REPO_HOME/storm/external/storm-solr/target/storm-solr-0.11.0-SNAPSHOT.jar` to `STORM_HOME/extlib`

**The code examples provided require that you first run the [Solr gettingstarted](http://lucene.apache.org/solr/quickstart.html) example** 

### Run Storm Topology

STORM_HOME/bin/storm jar REPO_HOME/storm/external/storm-solr/target/storm-solr-0.11.0-SNAPSHOT-tests.jar org.apache.storm.solr.topology.SolrFieldsTopology
 
STORM_HOME/bin/storm jar REPO_HOME/storm/external/storm-solr/target/storm-solr-0.11.0-SNAPSHOT-tests.jar org.apache.storm.solr.topology.SolrJsonTopology

### Run Trident Topology

STORM_HOME/bin/storm jar REPO_HOME/storm/external/storm-solr/target/storm-solr-0.11.0-SNAPSHOT-tests.jar org.apache.storm.solr.trident.SolrFieldsTridentTopology

STORM_HOME/bin/storm jar REPO_HOME/storm/external/storm-solr/target/storm-solr-0.11.0-SNAPSHOT-tests.jar org.apache.storm.solr.trident.SolrJsonTridentTopology


### Verify Results

The aforementioned Storm and Trident topologies index the Solr `gettingstarted` collection with objects that have the following `id` pattern:

\*id_fields_test_val\* for `SolrFieldsTopology` and  `SolrFieldsTridentTopology`

\*json_test_val\* for `SolrJsonTopology` and `SolrJsonTridentTopology`

Querying  Solr for these patterns, you will see the values that have been indexed by the Storm Apache Solr integration: 

curl -X GET -H "Content-type:application/json" -H "Accept:application/json" http://localhost:8983/solr/gettingstarted_shard1_replica2/select?q=*id_fields_test_val*&wt=json&indent=true

curl -X GET -H "Content-type: application/json" -H "Accept: application/json" http://localhost:8983/solr/gettingstarted_shard1_replica2/select?q=*id_fields_test_val*&wt=json&indent=true

You can also see the results by opening the Apache Solr UI and pasting the `id` pattern in the `q` textbox in the queries page

http://localhost:8983/solr/#/gettingstarted_shard1_replica2/query

