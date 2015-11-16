# Storm Elasticsearch Bolt & Trident State

  EsIndexBolt, EsPercolateBolt and EsState allows users to stream data from storm into Elasticsearch directly.
  For detailed description, please refer to the following.

## EsIndexBolt (org.apache.storm.elasticsearch.bolt.EsIndexBolt)

EsIndexBolt streams tuples directly into Elasticsearch. Tuples are indexed in specified index & type combination. 
Users should make sure that ```EsTupleMapper``` can extract "source", "index", "type", and "id" from input tuple.
"index" and "type" are used for identifying target index and type.
"source" is a document in JSON format string that will be indexed in Elasticsearch.

```java
EsConfig esConfig = new EsConfig(clusterName, new String[]{"localhost:9300"});
EsTupleMapper tupleMapper = new DefaultEsTupleMapper();
EsIndexBolt indexBolt = new EsIndexBolt(esConfig, tupleMapper);
```

## EsPercolateBolt (org.apache.storm.elasticsearch.bolt.EsPercolateBolt)

EsPercolateBolt streams tuples directly into Elasticsearch. Tuples are used to send percolate request to specified index & type combination. 
User should make sure ```EsTupleMapper``` can extract "source", "index", "type" from input tuple.
"index" and "type" are used for identifying target index and type.
"source" is a document in JSON format string that will be sent in percolate request to Elasticsearch.

```java
EsConfig esConfig = new EsConfig(clusterName, new String[]{"localhost:9300"});
EsTupleMapper tupleMapper = new DefaultEsTupleMapper();
EsPercolateBolt percolateBolt = new EsPercolateBolt(esConfig, tupleMapper);
```

If there exists non-empty percolate response, EsPercolateBolt will emit tuple with original source and Percolate.Match
for each Percolate.Match in PercolateResponse.

## EsState (org.apache.storm.elasticsearch.trident.EsState)

Elasticsearch Trident state also follows similar pattern to EsBolts. It takes in EsConfig and EsTupleMapper as an arg.

```java
EsConfig esConfig = new EsConfig(clusterName, new String[]{"localhost:9300"});
EsTupleMapper tupleMapper = new DefaultEsTupleMapper();

StateFactory factory = new EsStateFactory(esConfig, tupleMapper);
TridentState state = stream.partitionPersist(factory, esFields, new EsUpdater(), new Fields());
 ```

## EsLookupBolt (org.apache.storm.elasticsearch.bolt.EsLookupBolt)

EsLookupBolt performs a get request to Elasticsearch. 
In order to do that, three dependencies need to be satisfied. Apart from usual EsConfig, two other dependencies must be provided:
    ElasticsearchGetRequest is used to convert the incoming Tuple to the GetRequest that will be executed against Elasticsearch.
    EsLookupResultOutput is used to declare the output fields and convert the GetResponse to values that are emited by the bolt.

Incoming tuple is passed to provided GetRequest creator and the result of that execution is passed to Elasticsearch client.
The bolt then uses the provider output adapter (EsLookupResultOutput) to convert the GetResponse to Values to emit.
The output fields are also specified by the user of the bolt via the output adapter (EsLookupResultOutput).

```java
EsConfig esConfig = createEsConfig();
ElasticsearchGetRequest getRequestAdapter = createElasticsearchGetRequest();
EsLookupResultOutput output = createOutput();
EsLookupBolt lookupBolt = new EsLookupBolt(esConfig, getRequestAdapter, output);
```

## EsConfig (org.apache.storm.elasticsearch.common.EsConfig)
  
Provided components (Bolt, State) takes in EsConfig as a constructor arg.

```java
EsConfig esConfig = new EsConfig(clusterName, new String[]{"localhost:9300"});
```

or

```java
Map<String, String> additionalParameters = new HashMap<>();
additionalParameters.put("client.transport.sniff", "true");
EsConfig esConfig = new EsConfig(clusterName, new String[]{"localhost:9300"}, additionalParameters);
```

### EsConfig params

|Arg  |Description | Type
|---	|--- |---
|clusterName | Elasticsearch cluster name | String (required) |
|nodes | Elasticsearch nodes in a String array, each element should follow {host}:{port} pattern | String array (required) |
|additionalParameters | Additional Elasticsearch Transport Client configuration parameters | Map<String, String> (optional) |

## EsTupleMapper (org.apache.storm.elasticsearch.common.EsTupleMapper)

For storing tuple to Elasticsearch or percolating tuple from Elasticsearch, we need to define which fields are used for.
Users need to define your own by implementing ```EsTupleMapper```.
Storm-elasticsearch presents default mapper ```org.apache.storm.elasticsearch.common.DefaultEsTupleMapper```, which extracts its source, index, type, id values from identical fields.
You can refer implementation of DefaultEsTupleMapper to see how to implement your own.
  
## Committer Sponsors

 * Sriharsha Chintalapani ([@harshach](https://github.com/harshach))
 * Jungtaek Lim ([@HeartSaVioR](https://github.com/HeartSaVioR))
