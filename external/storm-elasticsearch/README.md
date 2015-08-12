# Storm Elasticsearch Bolt & Trident State

  EsIndexBolt, EsPercolateBolt and EsState allows users to stream data from storm into Elasticsearch directly.
  For detailed description, please refer to the following.

## EsIndexBolt (org.apache.storm.elasticsearch.bolt.EsIndexBolt)

EsIndexBolt streams tuples directly into Elasticsearch. Tuples are indexed in specified index & type combination. 
Users should make sure that ```EsTupleMapper``` can extract "source", "index", "type", and "id" from input tuple.
"index" and "type" are used for identifying target index and type.
"source" is a document in JSON format string that will be indexed in Elasticsearch.

```java
class SampleEsTupleMapper implements EsTupleMapper {
    @Override
    public String getSource(ITuple tuple) {
        return tuple.getStringByField("source");
    }

    @Override
    public String getIndex(ITuple tuple) {
        return tuple.getStringByField("index");
    }

    @Override
    public String getType(ITuple tuple) {
        return tuple.getStringByField("type");
    }

    @Override
    public String getId(ITuple tuple) {
        return tuple.getStringByField("id");
    }
}

EsConfig esConfig = new EsConfig();
esConfig.setClusterName(clusterName);
esConfig.setNodes(new String[]{"localhost:9300"});
EsTupleMapper tupleMapper = new SampleEsTupleMapper();
EsIndexBolt indexBolt = new EsIndexBolt(esConfig, tupleMapper);
```

## EsPercolateBolt (org.apache.storm.elasticsearch.bolt.EsPercolateBolt)

EsPercolateBolt streams tuples directly into Elasticsearch. Tuples are used to send percolate request to specified index & type combination. 
User should make sure ```EsTupleMapper``` can extract "source", "index", "type" from input tuple.
"index" and "type" are used for identifying target index and type.
"source" is a document in JSON format string that will be sent in percolate request to Elasticsearch.

```java
EsConfig esConfig = new EsConfig();
esConfig.setClusterName(clusterName);
esConfig.setNodes(new String[]{"localhost:9300"});
EsTupleMapper tupleMapper = new SampleEsTupleMapper();
EsPercolateBolt percolateBolt = new EsPercolateBolt(esConfig, tupleMapper);
```

If there exists non-empty percolate response, EsPercolateBolt will emit tuple with original source and Percolate.Match
for each Percolate.Match in PercolateResponse.

## EsState (org.apache.storm.elasticsearch.trident.EsState)

Elasticsearch Trident state also follows similar pattern to EsBolts. It takes in EsConfig and EsTupleMapper as an arg.

```code
   EsConfig esConfig = new EsConfig();
   esConfig.setClusterName(clusterName);
   esConfig.setNodes(new String[]{"localhost:9300"});
   EsTupleMapper tupleMapper = new SampleEsTupleMapper();

   StateFactory factory = new EsStateFactory(esConfig, tupleMapper);
   TridentState state = stream.partitionPersist(factory, esFields, new EsUpdater(), new Fields());
 ```

## EsConfig (org.apache.storm.elasticsearch.common.EsConfig)
  
Provided components (Bolt, State) takes in EsConfig as a constructor arg.

  ```java
   EsConfig esConfig = new EsConfig();
   esConfig.setClusterName(clusterName);
   esConfig.setNodes(new String[]{"localhost:9300"});
  ```

### EsConfig params

|Arg  |Description | Type
|---	|--- |---
|clusterName | Elasticsearch cluster name | String (required) |
|nodes | Elasticsearch nodes in a String array, each element should follow {host}:{port} pattern | String array (required) |

## EsTupleMapper (org.apache.storm.elasticsearch.common.EsTupleMapper)

For storing tuple to Elasticsearch or percolating tuple from Elasticsearch, we need to define which fields are used for.
Users need to define your own by implementing ```EsTupleMapper```. 
You can refer ```SampleEsTupleMapper``` above to see how to implement your own.
  
## Committer Sponsors

 * Sriharsha Chintalapani ([@harshach](https://github.com/harshach))
 * Jungtaek Lim ([@HeartSaVioR](https://github.com/HeartSaVioR))