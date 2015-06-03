# Storm ElasticSearch Bolt & Trident State

  EsIndexBolt, EsPercolateBolt and EsState allows users to stream data from storm into ElasticSearch directly.
  For detailed description, please refer to the following.   

## EsIndexBolt (org.apache.storm.elasticsearch.bolt.EsIndexBolt)

EsIndexBolt streams tuples directly into ElasticSearch. Tuples are indexed in specified index & type combination. 
User should make sure that there are "index","type", and "source" fields declared in preceding bolts or spout.
"index" and "type" fields are used for identifying target index and type.
"source" is a document in JSON format string that will be indexed in elastic search.

```java
EsConfig esConfig = new EsConfig();
esConfig.setClusterName(clusterName);
esConfig.setHost(new String[]{"localhost"});
esConfig.setPort(9300);
EsIndexBolt indexBolt = new IndexBolt(esConfig);
```

## EsPercolateBolt (org.apache.storm.elasticsearch.bolt.EsPercolateBolt)

EsPercolateBolt streams tuples directly into ElasticSearch. Tuples are used to send percolate request to specified index & type combination. 
User should make sure that there are "index","type", and "source" fields declared in preceding bolts or spout.
"index" and "type" fields are used for identifying target index and type.
"source" is a document in JSON format string that will be sent in percolate request to elastic search.

```java
EsConfig esConfig = new EsConfig();
esConfig.setClusterName(clusterName);
esConfig.setHost(new String[]{"localhost"});
esConfig.setPort(9300);
EsPercolateBolt percolateBolt = new EsPercolateBolt(esConfig);
```

### EsConfig (org.apache.storm.elasticsearch.common.EsConfig)
  
Two bolts above takes in EsConfig as a constructor arg.

  ```java
   EsConfig esConfig = new EsConfig();
   esConfig.setClusterName(clusterName);
   esConfig.setHost(new String[]{"localhost"});
   esConfig.setPort(9300);
  ```

EsConfig params

|Arg  |Description | Type
|---	|--- |---
|clusterName | ElasticSearch cluster name | String (required) |
|host | ElasticSearch host | String array (required) |
|port | ElasticSearch port | int (required) |


 
## EsState (org.apache.storm.elasticsearch.trident.EsState)

ElasticSearch Trident state also follows similar pattern to EsBolts. It takes in EsConfig as an arg.

```code
   EsConfig esConfig = new EsConfig();
   esConfig.setClusterName(clusterName);
   esConfig.setHost(new String[]{"localhost"});
   esConfig.setPort(9300);
                	     		
   StateFactory factory = new EsStateFactory(esConfig);
   TridentState state = stream.partitionPersist(factory, esFields, new EsUpdater(), new Fields());
 ```
  
## Committer Sponsors
