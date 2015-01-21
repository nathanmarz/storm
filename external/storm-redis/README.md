#Storm Redis

Storm/Trident integration for [Redis](http://redis.io/)

## Usage

### How do I use it?

use it as a maven dependency:

```xml
<dependency>
    <groupId>org.apache.storm</groupId>
    <artifactId>storm-redis</artifactId>
    <version>0.10.0</version>
    <type>jar</type>
</dependency>
```

### AbstractRedisBolt usage:

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
        TridentTupleMapper tupleMapper = new WordCountTupleMapper();
        RedisState.Factory factory = new RedisState.Factory(poolConfig);

        TridentTopology topology = new TridentTopology();
        Stream stream = topology.newStream("spout1", spout);

        stream.partitionPersist(factory,
                                fields,
                                new RedisStateUpdater("test_", tupleMapper, 86400000),
                                new Fields());
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
        TridentTupleMapper tupleMapper = new WordCountTupleMapper();
        RedisClusterState.Factory factory = new RedisClusterState.Factory(clusterConfig);

        TridentTopology topology = new TridentTopology();
        Stream stream = topology.newStream("spout1", spout);

        stream.partitionPersist(factory,
                                fields,
                                new RedisClusterStateUpdater("test_", tupleMapper, 86400000),
                                new Fields());
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

 * Bobby Evans ([bobby@apache.org](mailto:bobby@apache.org))
 * DashengJu ([dashengju@gmail.com](mailto:dashengju@gmail.com))
 * HeartSaVioR ([kabhwan@gmail.com](mailto:kabhwan@gmail.com))
 
