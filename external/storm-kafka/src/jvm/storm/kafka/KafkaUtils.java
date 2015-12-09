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
package storm.kafka;

import backtype.storm.metric.api.IMetric;
import backtype.storm.utils.Utils;
import com.google.common.base.Preconditions;
import kafka.api.FetchRequest;
import kafka.api.FetchRequestBuilder;
import kafka.api.PartitionOffsetRequestInfo;
import kafka.common.TopicAndPartition;
import kafka.javaapi.FetchResponse;
import kafka.javaapi.OffsetRequest;
import kafka.javaapi.consumer.SimpleConsumer;
import kafka.javaapi.message.ByteBufferMessageSet;
import kafka.message.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import storm.kafka.trident.GlobalPartitionInformation;
import storm.kafka.trident.IBrokerReader;
import storm.kafka.trident.StaticBrokerReader;
import storm.kafka.trident.ZkBrokerReader;

import java.io.IOException;
import java.net.ConnectException;
import java.net.SocketTimeoutException;
import java.nio.ByteBuffer;
import java.nio.channels.UnresolvedAddressException;
import java.util.*;


public class KafkaUtils {

    public static final Logger LOG = LoggerFactory.getLogger(KafkaUtils.class);
    private static final int NO_OFFSET = -5;


    public static IBrokerReader makeBrokerReader(Map stormConf, KafkaConfig conf) {
        if (conf.hosts instanceof StaticHosts) {
            return new StaticBrokerReader(conf.topic, ((StaticHosts) conf.hosts).getPartitionInformation());
        } else {
            return new ZkBrokerReader(stormConf, conf.topic, (ZkHosts) conf.hosts);
        }
    }


    public static long getOffset(SimpleConsumer consumer, String topic, int partition, KafkaConfig config) {
        long startOffsetTime = config.startOffsetTime;
        return getOffset(consumer, topic, partition, startOffsetTime);
    }

    public static long getOffset(SimpleConsumer consumer, String topic, int partition, long startOffsetTime) {
        TopicAndPartition topicAndPartition = new TopicAndPartition(topic, partition);
        Map<TopicAndPartition, PartitionOffsetRequestInfo> requestInfo = new HashMap<TopicAndPartition, PartitionOffsetRequestInfo>();
        requestInfo.put(topicAndPartition, new PartitionOffsetRequestInfo(startOffsetTime, 1));
        OffsetRequest request = new OffsetRequest(
                requestInfo, kafka.api.OffsetRequest.CurrentVersion(), consumer.clientId());

        long[] offsets = consumer.getOffsetsBefore(request).offsets(topic, partition);
        if (offsets.length > 0) {
            return offsets[0];
        } else {
            return NO_OFFSET;
        }
    }

    public static class KafkaOffsetMetric implements IMetric {
        Map<Partition, Long> _partitionToOffset = new HashMap<Partition, Long>();
        Set<Partition> _partitions;
        DynamicPartitionConnections _connections;

        public KafkaOffsetMetric(DynamicPartitionConnections connections) {
            _connections = connections;
        }

        public void setLatestEmittedOffset(Partition partition, long offset) {
            _partitionToOffset.put(partition, offset);
        }

        private class TopicMetrics {
            long totalSpoutLag = 0;
            long totalEarliestTimeOffset = 0;
            long totalLatestTimeOffset = 0;
            long totalLatestEmittedOffset = 0;
        }

        @Override
        public Object getValueAndReset() {
            try {
                HashMap ret = new HashMap();
                if (_partitions != null && _partitions.size() == _partitionToOffset.size()) {
                    Map<String,TopicMetrics> topicMetricsMap = new TreeMap<String, TopicMetrics>();
                    for (Map.Entry<Partition, Long> e : _partitionToOffset.entrySet()) {
                        Partition partition = e.getKey();
                        SimpleConsumer consumer = _connections.getConnection(partition);
                        if (consumer == null) {
                            LOG.warn("partitionToOffset contains partition not found in _connections. Stale partition data?");
                            return null;
                        }
                        long latestTimeOffset = getOffset(consumer, partition.topic, partition.partition, kafka.api.OffsetRequest.LatestTime());
                        long earliestTimeOffset = getOffset(consumer, partition.topic, partition.partition, kafka.api.OffsetRequest.EarliestTime());
                        if (latestTimeOffset == KafkaUtils.NO_OFFSET) {
                            LOG.warn("No data found in Kafka Partition " + partition.getId());
                            return null;
                        }
                        long latestEmittedOffset = e.getValue();
                        long spoutLag = latestTimeOffset - latestEmittedOffset;
                        String topic = partition.topic;
                        String metricPath = partition.getId();
                        //Handle the case where Partition Path Id does not contain topic name Partition.getId() == "partition_" + partition
                        if (!metricPath.startsWith(topic + "/")) {
                            metricPath = topic + "/" + metricPath;
                        }
                        ret.put(metricPath + "/" + "spoutLag", spoutLag);
                        ret.put(metricPath + "/" + "earliestTimeOffset", earliestTimeOffset);
                        ret.put(metricPath + "/" + "latestTimeOffset", latestTimeOffset);
                        ret.put(metricPath + "/" + "latestEmittedOffset", latestEmittedOffset);

                        if (!topicMetricsMap.containsKey(partition.topic)) {
                            topicMetricsMap.put(partition.topic,new TopicMetrics());
                        }

                        TopicMetrics topicMetrics = topicMetricsMap.get(partition.topic);
                        topicMetrics.totalSpoutLag += spoutLag;
                        topicMetrics.totalEarliestTimeOffset += earliestTimeOffset;
                        topicMetrics.totalLatestTimeOffset += latestTimeOffset;
                        topicMetrics.totalLatestEmittedOffset += latestEmittedOffset;
                    }

                    for(Map.Entry<String, TopicMetrics> e : topicMetricsMap.entrySet()) {
                        String topic = e.getKey();
                        TopicMetrics topicMetrics = e.getValue();
                        ret.put(topic + "/" + "totalSpoutLag", topicMetrics.totalSpoutLag);
                        ret.put(topic + "/" + "totalEarliestTimeOffset", topicMetrics.totalEarliestTimeOffset);
                        ret.put(topic + "/" + "totalLatestTimeOffset", topicMetrics.totalLatestTimeOffset);
                        ret.put(topic + "/" + "totalLatestEmittedOffset", topicMetrics.totalLatestEmittedOffset);
                    }

                    return ret;
                } else {
                    LOG.info("Metrics Tick: Not enough data to calculate spout lag.");
                }
            } catch (Throwable t) {
                LOG.warn("Metrics Tick: Exception when computing kafkaOffset metric.", t);
            }
            return null;
        }

        public void refreshPartitions(Set<Partition> partitions) {
            _partitions = partitions;
            Iterator<Partition> it = _partitionToOffset.keySet().iterator();
            while (it.hasNext()) {
                if (!partitions.contains(it.next())) {
                    it.remove();
                }
            }
        }
    }

    public static ByteBufferMessageSet fetchMessages(KafkaConfig config, SimpleConsumer consumer, Partition partition, long offset)
            throws TopicOffsetOutOfRangeException, FailedFetchException,RuntimeException {
        ByteBufferMessageSet msgs = null;
        String topic = partition.topic;
        int partitionId = partition.partition;
        FetchRequestBuilder builder = new FetchRequestBuilder();
        FetchRequest fetchRequest = builder.addFetch(topic, partitionId, offset, config.fetchSizeBytes).
                clientId(config.clientId).maxWait(config.fetchMaxWait).build();
        FetchResponse fetchResponse;
        try {
            fetchResponse = consumer.fetch(fetchRequest);
        } catch (Exception e) {
            if (e instanceof ConnectException ||
                    e instanceof SocketTimeoutException ||
                    e instanceof IOException ||
                    e instanceof UnresolvedAddressException
                    ) {
                LOG.warn("Network error when fetching messages:", e);
                throw new FailedFetchException(e);
            } else {
                throw new RuntimeException(e);
            }
        }
        if (fetchResponse.hasError()) {
            KafkaError error = KafkaError.getError(fetchResponse.errorCode(topic, partitionId));
            if (error.equals(KafkaError.OFFSET_OUT_OF_RANGE) && config.useStartOffsetTimeIfOffsetOutOfRange) {
                String msg = partition + " Got fetch request with offset out of range: [" + offset + "]";
                LOG.warn(msg);
                throw new TopicOffsetOutOfRangeException(msg);
            } else {
                String message = "Error fetching data from [" + partition + "] for topic [" + topic + "]: [" + error + "]";
                LOG.error(message);
                throw new FailedFetchException(message);
            }
        } else {
            msgs = fetchResponse.messageSet(topic, partitionId);
        }
        return msgs;
    }


    public static Iterable<List<Object>> generateTuples(KafkaConfig kafkaConfig, Message msg, String topic) {
        Iterable<List<Object>> tups;
        ByteBuffer payload = msg.payload();
        if (payload == null) {
            return null;
        }
        ByteBuffer key = msg.key();
        if (key != null && kafkaConfig.scheme instanceof KeyValueSchemeAsMultiScheme) {
            tups = ((KeyValueSchemeAsMultiScheme) kafkaConfig.scheme).deserializeKeyAndValue(key, payload);
        } else {
            if (kafkaConfig.scheme instanceof StringMultiSchemeWithTopic) {
                tups = ((StringMultiSchemeWithTopic)kafkaConfig.scheme).deserializeWithTopic(topic, payload);
            } else {
                tups = kafkaConfig.scheme.deserialize(payload);
            }
        }
        return tups;
    }
    
    public static Iterable<List<Object>> generateTuples(MessageMetadataSchemeAsMultiScheme scheme, Message msg, Partition partition, long offset) {
        ByteBuffer payload = msg.payload();
        if (payload == null) {
            return null;
        }
        return scheme.deserializeMessageWithMetadata(payload, partition, offset);
    }


    public static List<Partition> calculatePartitionsForTask(List<GlobalPartitionInformation> partitons, int totalTasks, int taskIndex) {
        Preconditions.checkArgument(taskIndex < totalTasks, "task index must be less that total tasks");
        List<Partition> taskPartitions = new ArrayList<Partition>();
        List<Partition> partitions = new ArrayList<Partition>();
        for(GlobalPartitionInformation partitionInformation : partitons) {
            partitions.addAll(partitionInformation.getOrderedPartitions());
        }
        int numPartitions = partitions.size();
        if (numPartitions < totalTasks) {
            LOG.warn("there are more tasks than partitions (tasks: " + totalTasks + "; partitions: " + numPartitions + "), some tasks will be idle");
        }
        for (int i = taskIndex; i < numPartitions; i += totalTasks) {
            Partition taskPartition = partitions.get(i);
            taskPartitions.add(taskPartition);
        }
        logPartitionMapping(totalTasks, taskIndex, taskPartitions);
        return taskPartitions;
    }

    private static void logPartitionMapping(int totalTasks, int taskIndex, List<Partition> taskPartitions) {
        String taskPrefix = taskId(taskIndex, totalTasks);
        if (taskPartitions.isEmpty()) {
            LOG.warn(taskPrefix + "no partitions assigned");
        } else {
            LOG.info(taskPrefix + "assigned " + taskPartitions);
        }
    }

    public static String taskId(int taskIndex, int totalTasks) {
        return "Task [" + (taskIndex + 1) + "/" + totalTasks + "] ";
    }
}
