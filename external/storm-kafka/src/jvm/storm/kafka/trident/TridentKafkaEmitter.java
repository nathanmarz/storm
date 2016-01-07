/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package storm.kafka.trident;

import backtype.storm.Config;
import backtype.storm.metric.api.CombinedMetric;
import backtype.storm.metric.api.MeanReducer;
import backtype.storm.metric.api.ReducedMetric;
import backtype.storm.task.TopologyContext;
import com.google.common.collect.ImmutableMap;
import kafka.javaapi.consumer.SimpleConsumer;
import kafka.javaapi.message.ByteBufferMessageSet;
import kafka.message.Message;
import kafka.message.MessageAndOffset;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import storm.kafka.*;
import storm.kafka.TopicOffsetOutOfRangeException;
import storm.trident.operation.TridentCollector;
import storm.trident.spout.IOpaquePartitionedTridentSpout;
import storm.trident.spout.IPartitionedTridentSpout;
import storm.trident.topology.TransactionAttempt;

import java.util.*;

public class TridentKafkaEmitter {

    public static final Logger LOG = LoggerFactory.getLogger(TridentKafkaEmitter.class);

    private DynamicPartitionConnections _connections;
    private String _topologyName;
    private KafkaUtils.KafkaOffsetMetric _kafkaOffsetMetric;
    private ReducedMetric _kafkaMeanFetchLatencyMetric;
    private CombinedMetric _kafkaMaxFetchLatencyMetric;
    private TridentKafkaConfig _config;
    private String _topologyInstanceId;

    public TridentKafkaEmitter(Map conf, TopologyContext context, TridentKafkaConfig config, String topologyInstanceId) {
        _config = config;
        _topologyInstanceId = topologyInstanceId;
        _connections = new DynamicPartitionConnections(_config, KafkaUtils.makeBrokerReader(conf, _config));
        _topologyName = (String) conf.get(Config.TOPOLOGY_NAME);
        _kafkaOffsetMetric = new KafkaUtils.KafkaOffsetMetric(_connections);
        context.registerMetric("kafkaOffset", _kafkaOffsetMetric, _config.metricsTimeBucketSizeInSecs);
        _kafkaMeanFetchLatencyMetric = context.registerMetric("kafkaFetchAvg", new MeanReducer(), _config.metricsTimeBucketSizeInSecs);
        _kafkaMaxFetchLatencyMetric = context.registerMetric("kafkaFetchMax", new MaxMetric(), _config.metricsTimeBucketSizeInSecs);
    }


    private Map failFastEmitNewPartitionBatch(TransactionAttempt attempt, TridentCollector collector, Partition partition, Map lastMeta) {
        SimpleConsumer consumer = _connections.register(partition);
        Map ret = doEmitNewPartitionBatch(consumer, partition, collector, lastMeta);
        _kafkaOffsetMetric.setLatestEmittedOffset(partition, (Long) ret.get("offset"));
        return ret;
    }

    private Map emitNewPartitionBatch(TransactionAttempt attempt, TridentCollector collector, Partition partition, Map lastMeta) {
        try {
            return failFastEmitNewPartitionBatch(attempt, collector, partition, lastMeta);
        } catch (FailedFetchException e) {
            LOG.warn("Failed to fetch from partition " + partition);
            if (lastMeta == null) {
                return null;
            } else {
                Map ret = new HashMap();
                ret.put("offset", lastMeta.get("nextOffset"));
                ret.put("nextOffset", lastMeta.get("nextOffset"));
                ret.put("partition", partition.partition);
                ret.put("broker", ImmutableMap.of("host", partition.host.host, "port", partition.host.port));
                ret.put("topic", partition.topic);
                ret.put("topology", ImmutableMap.of("name", _topologyName, "id", _topologyInstanceId));
                return ret;
            }
        }
    }

    private Map doEmitNewPartitionBatch(SimpleConsumer consumer, Partition partition, TridentCollector collector, Map lastMeta) {
        long offset;
        if (lastMeta != null) {
            String lastInstanceId = null;
            Map lastTopoMeta = (Map) lastMeta.get("topology");
            if (lastTopoMeta != null) {
                lastInstanceId = (String) lastTopoMeta.get("id");
            }
            if (_config.ignoreZkOffsets && !_topologyInstanceId.equals(lastInstanceId)) {
                offset = KafkaUtils.getOffset(consumer, partition.topic, partition.partition, _config.startOffsetTime);
            } else {
                offset = (Long) lastMeta.get("nextOffset");
            }
        } else {
            offset = KafkaUtils.getOffset(consumer, partition.topic, partition.partition, _config);
        }

        ByteBufferMessageSet msgs = null;
        try {
            msgs = fetchMessages(consumer, partition, offset);
        } catch (TopicOffsetOutOfRangeException e) {
            long newOffset = KafkaUtils.getOffset(consumer, partition.topic, partition.partition, kafka.api.OffsetRequest.EarliestTime());
            LOG.warn("OffsetOutOfRange: Updating offset from offset = " + offset + " to offset = " + newOffset);
            offset = newOffset;
            msgs = KafkaUtils.fetchMessages(_config, consumer, partition, offset);
        }

        long endoffset = offset;
        for (MessageAndOffset msg : msgs) {
            emit(collector, msg.message(), partition, msg.offset());
            endoffset = msg.nextOffset();
        }
        Map newMeta = new HashMap();
        newMeta.put("offset", offset);
        newMeta.put("nextOffset", endoffset);
        newMeta.put("instanceId", _topologyInstanceId);
        newMeta.put("partition", partition.partition);
        newMeta.put("broker", ImmutableMap.of("host", partition.host.host, "port", partition.host.port));
        newMeta.put("topic", partition.topic);
        newMeta.put("topology", ImmutableMap.of("name", _topologyName, "id", _topologyInstanceId));
        return newMeta;
    }

    private ByteBufferMessageSet fetchMessages(SimpleConsumer consumer, Partition partition, long offset) {
        long start = System.nanoTime();
        ByteBufferMessageSet msgs = null;
        msgs = KafkaUtils.fetchMessages(_config, consumer, partition, offset);
        long end = System.nanoTime();
        long millis = (end - start) / 1000000;
        _kafkaMeanFetchLatencyMetric.update(millis);
        _kafkaMaxFetchLatencyMetric.update(millis);
        return msgs;
    }

    /**
     * re-emit the batch described by the meta data provided
     *
     * @param attempt
     * @param collector
     * @param partition
     * @param meta
     */
    private void reEmitPartitionBatch(TransactionAttempt attempt, TridentCollector collector, Partition partition, Map meta) {
        LOG.info("re-emitting batch, attempt " + attempt);
        String instanceId = (String) meta.get("instanceId");
        if (!_config.ignoreZkOffsets || instanceId.equals(_topologyInstanceId)) {
            SimpleConsumer consumer = _connections.register(partition);
            long offset = (Long) meta.get("offset");
            long nextOffset = (Long) meta.get("nextOffset");
            ByteBufferMessageSet msgs = null;
            msgs = fetchMessages(consumer, partition, offset);

            if(msgs != null) {
                for (MessageAndOffset msg : msgs) {
                    if (offset == nextOffset) {
                        break;
                    }
                    if (offset > nextOffset) {
                        throw new RuntimeException("Error when re-emitting batch. overshot the end offset");
                    }
                    emit(collector, msg.message(), partition, msg.offset());
                    offset = msg.nextOffset();
                }
            }
        }
    }

    private void emit(TridentCollector collector, Message msg, Partition partition, long offset) {
        Iterable<List<Object>> values;
        if (_config.scheme instanceof MessageMetadataSchemeAsMultiScheme) {
            values = KafkaUtils.generateTuples((MessageMetadataSchemeAsMultiScheme) _config.scheme, msg, partition, offset);
        } else {
            values = KafkaUtils.generateTuples(_config, msg, partition.topic);
        }

        if (values != null) {
            for (List<Object> value : values) {
                collector.emit(value);
            }
        }
    }

    private void clear() {
        _connections.clear();
    }

    private List<Partition> orderPartitions(List<GlobalPartitionInformation> partitions) {
        List<Partition> part = new ArrayList<Partition>();
        for (GlobalPartitionInformation globalPartitionInformation : partitions)
            part.addAll(globalPartitionInformation.getOrderedPartitions());
        return part;
    }

    private void refresh(List<Partition> list) {
        _connections.clear();
        _kafkaOffsetMetric.refreshPartitions(new HashSet<Partition>(list));
    }


    public IOpaquePartitionedTridentSpout.Emitter<List<GlobalPartitionInformation>, Partition, Map> asOpaqueEmitter() {

        return new IOpaquePartitionedTridentSpout.Emitter<List<GlobalPartitionInformation>, Partition, Map>() {

            /**
             * Emit a batch of tuples for a partition/transaction.
             *
             * Return the metadata describing this batch that will be used as lastPartitionMeta
             * for defining the parameters of the next batch.
             */
            @Override
            public Map emitPartitionBatch(TransactionAttempt transactionAttempt, TridentCollector tridentCollector, Partition partition, Map map) {
                return emitNewPartitionBatch(transactionAttempt, tridentCollector, partition, map);
            }

            @Override
            public void refreshPartitions(List<Partition> partitions) {
                refresh(partitions);
            }

            @Override
            public List<Partition> getOrderedPartitions(List<GlobalPartitionInformation> partitionInformation) {
                return orderPartitions(partitionInformation);
            }

            @Override
            public void close() {
                clear();
            }
        };
    }

    public IPartitionedTridentSpout.Emitter asTransactionalEmitter() {
        return new IPartitionedTridentSpout.Emitter<List<GlobalPartitionInformation>, Partition, Map>() {

            /**
             * Emit a batch of tuples for a partition/transaction that's never been emitted before.
             * Return the metadata that can be used to reconstruct this partition/batch in the future.
             */
            @Override
            public Map emitPartitionBatchNew(TransactionAttempt transactionAttempt, TridentCollector tridentCollector, Partition partition, Map map) {
                return failFastEmitNewPartitionBatch(transactionAttempt, tridentCollector, partition, map);
            }

            /**
             * Emit a batch of tuples for a partition/transaction that has been emitted before, using
             * the metadata created when it was first emitted.
             */
            @Override
            public void emitPartitionBatch(TransactionAttempt transactionAttempt, TridentCollector tridentCollector, Partition partition, Map map) {
                reEmitPartitionBatch(transactionAttempt, tridentCollector, partition, map);
            }

            /**
             * This method is called when this task is responsible for a new set of partitions. Should be used
             * to manage things like connections to brokers.
             */
            @Override
            public void refreshPartitions(List<Partition> partitions) {
                refresh(partitions);
            }

            @Override
            public List<Partition> getOrderedPartitions(List<GlobalPartitionInformation> partitionInformation) {
                return orderPartitions(partitionInformation);
            }

            @Override
            public void close() {
                clear();
            }
        };

    }


}
