# Storm Elasticsearch Bolt & Trident State

  EsIndexBolt, EsPercolateBolt and EsState allows users to stream data from storm into Elasticsearch directly.
  For detailed description, please refer to the following.

## EsIndexBolt (org.apache.storm.elasticsearch.bolt.EsIndexBolt)

EsIndexBolt streams tuples directly into Elasticsearch. Tuples are indexed in specified index & type combination. 
User should make sure that there are "source", "index","type", and "id" fields declared in preceding bolts or spout.
"index" and "type" fields are used for identifying target index and type.
"source" is a document in JSON format string that will be indexed in Elasticsearch.

```java
EsConfig esConfig = new EsConfig();
esConfig.setClusterName(clusterName);
esConfig.setNodes(new String[]{"localhost:9300"});
EsIndexBolt indexBolt = new EsIndexBolt(esConfig);
```

## EsPercolateBolt (org.apache.storm.elasticsearch.bolt.EsPercolateBolt)

EsPercolateBolt streams tuples directly into Elasticsearch. Tuples are used to send percolate request to specified index & type combination. 
User should make sure that there are "source", "index", and "type" fields declared in preceding bolts or spout.
"index" and "type" fields are used for identifying target index and type.
"source" is a document in JSON format string that will be sent in percolate request to Elasticsearch.

```java
EsConfig esConfig = new EsConfig();
esConfig.setClusterName(clusterName);
esConfig.setNodes(new String[]{"localhost:9300"});
EsPercolateBolt percolateBolt = new EsPercolateBolt(esConfig);
```

If there exists non-empty percolate response, EsPercolateBolt will emit tuple with original source and Percolate.Match
for each Percolate.Match in PercolateResponse.

## EsConfig (org.apache.storm.elasticsearch.common.EsConfig)
  
Two bolts above takes in EsConfig as a constructor arg.

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


 
## EsState (org.apache.storm.elasticsearch.trident.EsState)

Elasticsearch Trident state also follows similar pattern to EsBolts. It takes in EsConfig as an arg.

```code
   EsConfig esConfig = new EsConfig();
   esConfig.setClusterName(clusterName);
   esConfig.setNodes(new String[]{"localhost:9300"});

   StateFactory factory = new EsStateFactory(esConfig);
   TridentState state = stream.partitionPersist(factory, esFields, new EsUpdater(), new Fields());
 ```
  
## Committer Sponsors

 * Sriharsha Chintalapani ([@harshach](https://github.com/harshach))
 * Jungtaek Lim ([@HeartSaVioR](https://github.com/HeartSaVioR))