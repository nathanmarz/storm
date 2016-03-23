---
title: Storm Redis Integration
layout: documentation
documentation: true
---

Storm/Trident integration for [Redis](http://redis.io/)

Storm-redis uses Jedis for Redis client.

## Usage

### How do I use it?

use it as a maven dependency:

```xml
<dependency>
    <groupId>org.apache.storm</groupId>
    <artifactId>storm-redis</artifactId>
    <version>${storm.version}</version>
    <type>jar</type>
</dependency>
```

### For normal Bolt

Storm-redis provides basic Bolt implementations, ```RedisLookupBolt``` and ```RedisStoreBolt```.

As name represents its usage, ```RedisLookupBolt``` retrieves value from Redis using key, and ```RedisStoreBolt``` stores key / value to Redis. One tuple will be matched to one key / value pair, and you can define match pattern to ```TupleMapper```.

You can also choose data type from ```RedisDataTypeDescription``` to use. Please refer ```RedisDataTypeDescription.RedisDataType``` to see what data types are supported. In some data types (hash and sorted set), it requires additional key and converted key from tuple becomes element.

These interfaces are combined with ```RedisLookupMapper``` and ```RedisStoreMapper``` which fit ```RedisLookupBolt``` and ```RedisStoreBolt``` respectively.

#### RedisLookupBolt example

```java

class WordCountRedisLookupMapper implements RedisLookupMapper {
    private RedisDataTypeDescription description;
    private final String hashKey = "wordCount";

    public WordCountRedisLookupMapper() {
        description = new RedisDataTypeDescription(
                RedisDataTypeDescription.RedisDataType.HASH, hashKey);
    }

    @Override
    public List<Values> toTuple(ITuple input, Object value) {
        String member = getKeyFromTuple(input);
        List<Values> values = Lists.newArrayList();
        values.add(new Values(member, value));
        return values;
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("wordName", "count"));
    }

    @Override
    public RedisDataTypeDescription getDataTypeDescription() {
        return description;
    }

    @Override
    public String getKeyFromTuple(ITuple tuple) {
        return tuple.getStringByField("word");
    }

    @Override
    public String getValueFromTuple(ITuple tuple) {
        return null;
    }
}

```

```java

JedisPoolConfig poolConfig = new JedisPoolConfig.Builder()
        .setHost(host).setPort(port).build();
RedisLookupMapper lookupMapper = new WordCountRedisLookupMapper();
RedisLookupBolt lookupBolt = new RedisLookupBolt(poolConfig, lookupMapper);
```

#### RedisStoreBolt example

```java

class WordCountStoreMapper implements RedisStoreMapper {
    private RedisDataTypeDescription description;
    private final String hashKey = "wordCount";

    public WordCountStoreMapper() {
        description = new RedisDataTypeDescription(
            RedisDataTypeDescription.RedisDataType.HASH, hashKey);
    }

    @Override
    public RedisDataTypeDescription getDataTypeDescription() {
        return description;
    }

    @Override
    public String getKeyFromTuple(ITuple tuple) {
        return tuple.getStringByField("word");
    }

    @Override
    public String getValueFromTuple(ITuple tuple) {
        return tuple.getStringByField("count");
    }
}
```

```java

JedisPoolConfig poolConfig = new JedisPoolConfig.Builder()
                .setHost(host).setPort(port).build();
RedisStoreMapper storeMapper = new WordCountStoreMapper();
RedisStoreBolt storeBolt = new RedisStoreBolt(poolConfig, storeMapper);
```

### For non-simple Bolt

If your scenario doesn't fit ```RedisStoreBolt``` and ```RedisLookupBolt```, storm-redis also provides ```AbstractRedisBolt``` to let you extend and apply your business logic.

```java

    public static class LookupWordTotalCountBolt extends AbstractRedisBolt {
        private static final Logger LOG = LoggerFactory.getLogger(LookupWordTotalCountBolt.class);
        private static final Random RANDOM = new Random();

        public LookupWordTotalCountBolt(JedisPoolConfig config) {
            super(config);
        }

        public LookupWordTotalCountBolt(JedisClusterConfig config) {
            super(config);
        }

        @Override
        public void execute(Tuple input) {
            JedisCommands jedisCommands = null;
            try {
                jedisCommands = getInstance();
                String wordName = input.getStringByField("word");
                String countStr = jedisCommands.get(wordName);
                if (countStr != null) {
                    int count = Integer.parseInt(countStr);
                    this.collector.emit(new Values(wordName, count));

                    // print lookup result with low probability
                    if(RANDOM.nextInt(1000) > 995) {
                        LOG.info("Lookup result - word : " + wordName + " / count : " + count);
                    }
                } else {
                    // skip
                    LOG.warn("Word not found in Redis - word : " + wordName);
                }
            } finally {
                if (jedisCommands != null) {
                    returnInstance(jedisCommands);
                }
                this.collector.ack(input);
            }
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            // wordName, count
            declarer.declare(new Fields("wordName", "count"));
        }
    }

```

### Trident State usage

1. RedisState and RedisMapState, which provide Jedis interface just for single redis.

2. RedisClusterState and RedisClusterMapState, which provide JedisCluster interface, just for redis cluster.

RedisState
```java
        JedisPoolConfig poolConfig = new JedisPoolConfig.Builder()
                                        .setHost(redisHost).setPort(redisPort)
                                        .build();
        RedisStoreMapper storeMapper = new WordCountStoreMapper();
        RedisLookupMapper lookupMapper = new WordCountLookupMapper();
        RedisState.Factory factory = new RedisState.Factory(poolConfig);

        TridentTopology topology = new TridentTopology();
        Stream stream = topology.newStream("spout1", spout);

        stream.partitionPersist(factory,
                                fields,
                                new RedisStateUpdater(storeMapper).withExpire(86400000),
                                new Fields());

        TridentState state = topology.newStaticState(factory);
        stream = stream.stateQuery(state, new Fields("word"),
                                new RedisStateQuerier(lookupMapper),
                                new Fields("columnName","columnValue"));
```

RedisClusterState
```java
        Set<InetSocketAddress> nodes = new HashSet<InetSocketAddress>();
        for (String hostPort : redisHostPort.split(",")) {
            String[] host_port = hostPort.split(":");
            nodes.add(new InetSocketAddress(host_port[0], Integer.valueOf(host_port[1])));
        }
        JedisClusterConfig clusterConfig = new JedisClusterConfig.Builder().setNodes(nodes)
                                        .build();
        RedisStoreMapper storeMapper = new WordCountStoreMapper();
        RedisLookupMapper lookupMapper = new WordCountLookupMapper();
        RedisClusterState.Factory factory = new RedisClusterState.Factory(clusterConfig);

        TridentTopology topology = new TridentTopology();
        Stream stream = topology.newStream("spout1", spout);

        stream.partitionPersist(factory,
                                fields,
                                new RedisClusterStateUpdater(storeMapper).withExpire(86400000),
                                new Fields());

        TridentState state = topology.newStaticState(factory);
        stream = stream.stateQuery(state, new Fields("word"),
                                new RedisClusterStateQuerier(lookupMapper),
                                new Fields("columnName","columnValue"));
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

 * Robert Evans ([@revans2](https://github.com/revans2))
 * Jungtaek Lim ([@HeartSaVioR](https://github.com/HeartSaVioR))
