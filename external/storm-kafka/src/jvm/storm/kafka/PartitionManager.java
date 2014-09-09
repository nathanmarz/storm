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

import backtype.storm.Config;
import backtype.storm.metric.api.CombinedMetric;
import backtype.storm.metric.api.CountMetric;
import backtype.storm.metric.api.MeanReducer;
import backtype.storm.metric.api.ReducedMetric;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.utils.Utils;
import com.google.common.collect.ImmutableMap;
import kafka.javaapi.consumer.SimpleConsumer;
import kafka.javaapi.message.ByteBufferMessageSet;
import kafka.message.MessageAndOffset;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import storm.kafka.KafkaSpout.EmitState;
import storm.kafka.KafkaSpout.MessageAndRealOffset;
import storm.kafka.trident.MaxMetric;

import java.util.*;

public class PartitionManager {
    public static final Logger LOG = LoggerFactory.getLogger(PartitionManager.class);
    private final CombinedMetric _fetchAPILatencyMax;
    private final ReducedMetric _fetchAPILatencyMean;
    private final CountMetric _fetchAPICallCount;
    private final CountMetric _fetchAPIMessageCount;
    Long _emittedToOffset;
    SortedSet<Long> _pending = new TreeSet<Long>();
    private SortedSet<Long> failed = new TreeSet<Long>();
    private Map<Long,MessageRetryRecord> retryRecords = new HashMap<Long,MessageRetryRecord>();
    Long _committedTo;
    LinkedList<MessageAndRealOffset> _waitingToEmit = new LinkedList<MessageAndRealOffset>();
    Partition _partition;
    SpoutConfig _spoutConfig;
    String _topologyInstanceId;
    SimpleConsumer _consumer;
    DynamicPartitionConnections _connections;
    ZkState _state;
    Map _stormConf;
    long numberFailed, numberAcked;
    public PartitionManager(DynamicPartitionConnections connections, String topologyInstanceId, ZkState state, Map stormConf, SpoutConfig spoutConfig, Partition id) {
        _partition = id;
        _connections = connections;
        _spoutConfig = spoutConfig;
        _topologyInstanceId = topologyInstanceId;
        _consumer = connections.register(id.host, id.partition);
        _state = state;
        _stormConf = stormConf;
        numberAcked = numberFailed = 0;

        String jsonTopologyId = null;
        Long jsonOffset = null;
        String path = committedPath();
        try {
            Map<Object, Object> json = _state.readJSON(path);
            LOG.info("Read partition information from: " + path +  "  --> " + json );
            if (json != null) {
                jsonTopologyId = (String) ((Map<Object, Object>) json.get("topology")).get("id");
                jsonOffset = (Long) json.get("offset");
            }
        } catch (Throwable e) {
            LOG.warn("Error reading and/or parsing at ZkNode: " + path, e);
        }

        Long currentOffset = KafkaUtils.getOffset(_consumer, spoutConfig.topic, id.partition, spoutConfig);

        if (jsonTopologyId == null || jsonOffset == null) { // failed to parse JSON?
            _committedTo = currentOffset;
            LOG.info("No partition information found, using configuration to determine offset");
        } else if (!topologyInstanceId.equals(jsonTopologyId) && spoutConfig.forceFromStart) {
            _committedTo = KafkaUtils.getOffset(_consumer, spoutConfig.topic, id.partition, spoutConfig.startOffsetTime);
            LOG.info("Topology change detected and reset from start forced, using configuration to determine offset");
        } else {
            _committedTo = jsonOffset;
            LOG.info("Read last commit offset from zookeeper: " + _committedTo + "; old topology_id: " + jsonTopologyId + " - new topology_id: " + topologyInstanceId );
        }

        if (currentOffset - _committedTo > spoutConfig.maxOffsetBehind || _committedTo <= 0) {
            LOG.info("Last commit offset from zookeeper: " + _committedTo);
            _committedTo = currentOffset;
            LOG.info("Commit offset " + _committedTo + " is more than " +
                    spoutConfig.maxOffsetBehind + " behind, resetting to startOffsetTime=" + spoutConfig.startOffsetTime);
        }

        LOG.info("Starting Kafka " + _consumer.host() + ":" + id.partition + " from offset " + _committedTo);
        _emittedToOffset = _committedTo;

        _fetchAPILatencyMax = new CombinedMetric(new MaxMetric());
        _fetchAPILatencyMean = new ReducedMetric(new MeanReducer());
        _fetchAPICallCount = new CountMetric();
        _fetchAPIMessageCount = new CountMetric();
    }

    public Map getMetricsDataMap() {
        Map ret = new HashMap();
        ret.put(_partition + "/fetchAPILatencyMax", _fetchAPILatencyMax.getValueAndReset());
        ret.put(_partition + "/fetchAPILatencyMean", _fetchAPILatencyMean.getValueAndReset());
        ret.put(_partition + "/fetchAPICallCount", _fetchAPICallCount.getValueAndReset());
        ret.put(_partition + "/fetchAPIMessageCount", _fetchAPIMessageCount.getValueAndReset());
        return ret;
    }

    //returns false if it's reached the end of current batch
    public EmitState next(SpoutOutputCollector collector) {
        if (_waitingToEmit.isEmpty()) {
            fill();
        }
        while (true) {
            MessageAndRealOffset toEmit = _waitingToEmit.pollFirst();
            if (toEmit == null) {
                return EmitState.NO_EMITTED;
            }
            Iterable<List<Object>> tups = KafkaUtils.generateTuples(_spoutConfig, toEmit.msg);
            if (tups != null) {
                for (List<Object> tup : tups) {
                    collector.emit(tup, new KafkaMessageId(_partition, toEmit.offset));
                }
                break;
            } else {
                ack(toEmit.offset);
            }
        }
        if (!_waitingToEmit.isEmpty()) {
            return EmitState.EMITTED_MORE_LEFT;
        } else {
            return EmitState.EMITTED_END;
        }
    }

    /**
     * Fetch the failed messages ready for retry.  If there are no failed messages, or none are ready for retry, then it
     * returns an empty List (i.e., not null).
     */
    private SortedSet<Long> failedMsgsReadyForRetry() {
        SortedSet<Long> ready = new TreeSet<Long>();
        for (Long offset : this.failed) {
            if (this.retryRecords.get(offset).isReadyForRetry()) {
                ready.add(offset);
            }
        }
        return ready;
    }


    private void fill() {
        long start = System.nanoTime();
        long offset;
        final SortedSet<Long> failedReady = failedMsgsReadyForRetry();

        // Are there failed tuples? If so, fetch those first.
        final boolean had_failed = !failedReady.isEmpty();
        if (had_failed) {
            offset = failedReady.first();
        } else {
            offset = _emittedToOffset;
        }

        ByteBufferMessageSet msgs = KafkaUtils.fetchMessages(_spoutConfig, _consumer, _partition, offset);
        long end = System.nanoTime();
        long millis = (end - start) / 1000000;
        _fetchAPILatencyMax.update(millis);
        _fetchAPILatencyMean.update(millis);
        _fetchAPICallCount.incr();
        if (msgs != null) {
            int numMessages = 0;

            for (MessageAndOffset msg : msgs) {
                final Long cur_offset = msg.offset();
                if (cur_offset < offset) {
                    // Skip any old offsets.
                    continue;
                }
                if (!had_failed || failedReady.contains(cur_offset)) {
                    numMessages += 1;
                    _pending.add(cur_offset);
                    _waitingToEmit.add(new MessageAndRealOffset(msg.message(), cur_offset));
                    _emittedToOffset = Math.max(msg.nextOffset(), _emittedToOffset);
                    if (had_failed) {
                        failed.remove(cur_offset);
                    }
                }
            }
            _fetchAPIMessageCount.incrBy(numMessages);
        }
    }

    public void ack(Long offset) {
        if (!_pending.isEmpty() && _pending.first() < offset - _spoutConfig.maxOffsetBehind) {
            // Too many things pending!
            _pending.headSet(offset - _spoutConfig.maxOffsetBehind).clear();
        }
        _pending.remove(offset);
        retryRecords.remove(offset);
        numberAcked++;
    }

    public void fail(Long offset) {
        if (offset < _emittedToOffset - _spoutConfig.maxOffsetBehind) {
            LOG.info(
                    "Skipping failed tuple at offset=" + offset +
                            " because it's more than maxOffsetBehind=" + _spoutConfig.maxOffsetBehind +
                            " behind _emittedToOffset=" + _emittedToOffset
            );
        } else {
            LOG.debug("failing at offset=" + offset + " with _pending.size()=" + _pending.size() + " pending and _emittedToOffset=" + _emittedToOffset);
            failed.add(offset);
            MessageRetryRecord retryRecord = retryRecords.get(offset);
            retryRecords.put(offset, retryRecord == null
                                     ? new MessageRetryRecord()
                                     : retryRecord.createNextRetryRecord());
            numberFailed++;
            if (numberAcked == 0 && numberFailed > _spoutConfig.maxOffsetBehind) {
                throw new RuntimeException("Too many tuple failures");
            }
        }
    }

    public void commit() {
        long lastCompletedOffset = lastCompletedOffset();
        if (_committedTo != lastCompletedOffset) {
            LOG.debug("Writing last completed offset (" + lastCompletedOffset + ") to ZK for " + _partition + " for topology: " + _topologyInstanceId);
            Map<Object, Object> data = (Map<Object, Object>) ImmutableMap.builder()
                    .put("topology", ImmutableMap.of("id", _topologyInstanceId,
                            "name", _stormConf.get(Config.TOPOLOGY_NAME)))
                    .put("offset", lastCompletedOffset)
                    .put("partition", _partition.partition)
                    .put("broker", ImmutableMap.of("host", _partition.host.host,
                            "port", _partition.host.port))
                    .put("topic", _spoutConfig.topic).build();
            _state.writeJSON(committedPath(), data);

            _committedTo = lastCompletedOffset;
            LOG.debug("Wrote last completed offset (" + lastCompletedOffset + ") to ZK for " + _partition + " for topology: " + _topologyInstanceId);
        } else {
            LOG.debug("No new offset for " + _partition + " for topology: " + _topologyInstanceId);
        }
    }

    private String committedPath() {
        return _spoutConfig.zkRoot + "/" + _spoutConfig.id + "/" + _partition.getId();
    }

    public long lastCompletedOffset() {
        if (_pending.isEmpty()) {
            return _emittedToOffset;
        } else {
            return _pending.first();
        }
    }

    public Partition getPartition() {
        return _partition;
    }

    public void close() {
        _connections.unregister(_partition.host, _partition.partition);
    }

    static class KafkaMessageId {
        public Partition partition;
        public long offset;

        public KafkaMessageId(Partition partition, long offset) {
            this.partition = partition;
            this.offset = offset;
        }
    }

    /**
     * A MessageRetryRecord holds the data of how many times a message has
     * failed and been retried, and when the last failure occurred.  It can
     * determine whether it is ready to be retried by employing an exponential
     * back-off calculation using config values stored in SpoutConfig:
     * <ul>
     *  <li>retryInitialDelayMs - time to delay before the first retry</li>
     *  <li>retryDelayMultiplier - multiplier by which to increase the delay for each subsequent retry</li>
     *  <li>retryDelayMaxMs - maximum retry delay (once this delay time is reached, subsequent retries will
     *                        delay for this amount of time every time)
     *  </li>
     * </ul>
     */
    class MessageRetryRecord {
        private final int retryNum;
        private final long retryTimeUTC;

        public MessageRetryRecord() {
            this(1);
        }

        private MessageRetryRecord(int retryNum) {
            this.retryNum = retryNum;
            this.retryTimeUTC = new Date().getTime() + calculateRetryDelay();
        }

        public MessageRetryRecord createNextRetryRecord() {
            return new MessageRetryRecord(this.retryNum + 1);
        }

        private long calculateRetryDelay() {
            double delayMultiplier = Math.pow(_spoutConfig.retryDelayMultiplier, this.retryNum - 1);
            long delayThisRetryMs = (long) (_spoutConfig.retryInitialDelayMs * delayMultiplier);
            return Math.min(delayThisRetryMs, _spoutConfig.retryDelayMaxMs);
        }

        public boolean isReadyForRetry() {
            return new Date().getTime() > this.retryTimeUTC;
        }
    }
}
