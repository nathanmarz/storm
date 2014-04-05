package storm.kafka;

import backtype.storm.Config;
import backtype.storm.metric.api.CombinedMetric;
import backtype.storm.metric.api.CountMetric;
import backtype.storm.metric.api.MeanReducer;
import backtype.storm.metric.api.ReducedMetric;
import backtype.storm.spout.SpoutOutputCollector;
import com.google.common.collect.ImmutableMap;
import kafka.api.OffsetRequest;
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

    static class KafkaMessageId {
        public Partition partition;
        public long offset;

        public KafkaMessageId(Partition partition, long offset) {
            this.partition = partition;
            this.offset = offset;
        }
    }

    Long _emittedToOffset;
    SortedSet<Long> _pending = new TreeSet<Long>();
    Long _committedTo;
    LinkedList<MessageAndRealOffset> _waitingToEmit = new LinkedList<MessageAndRealOffset>();
    Partition _partition;
    SpoutConfig _spoutConfig;
    String _topologyInstanceId;
    SimpleConsumer _consumer;
    DynamicPartitionConnections _connections;
    ZkState _state;
    Map _stormConf;


    public PartitionManager(DynamicPartitionConnections connections, String topologyInstanceId, ZkState state, Map stormConf, SpoutConfig spoutConfig, Partition id) {
        _partition = id;
        _connections = connections;
        _spoutConfig = spoutConfig;
        _topologyInstanceId = topologyInstanceId;
        _consumer = connections.register(id.host, id.partition);
        _state = state;
        _stormConf = stormConf;

        String jsonTopologyId = null;
        Long jsonOffset = null;
        String path = committedPath();
        try {
            Map<Object, Object> json = _state.readJSON(path);
            LOG.info("Read partition information from: " + path +  " --> " + json );
            if (json != null) {
                jsonTopologyId = (String) ((Map<Object, Object>) json.get("topology")).get("id");
                jsonOffset = (Long) json.get("offset");
            }
        } catch (Throwable e) {
            LOG.warn("Error reading and/or parsing at ZkNode: " + path, e);
        }

        if (jsonTopologyId == null || jsonOffset == null) { // failed to parse JSON?
            _committedTo = KafkaUtils.getOffset(_consumer, spoutConfig.topic, id.partition, spoutConfig);
            LOG.info("No partition information found, using configuration to determine offset");
        } else if (!topologyInstanceId.equals(jsonTopologyId) && spoutConfig.forceFromStart) {
            _committedTo = KafkaUtils.getOffset(_consumer, spoutConfig.topic, id.partition, spoutConfig.startOffsetTime);
            LOG.info("Topology change detected and reset from start forced, using configuration to determine offset");
        } else {
            _committedTo = jsonOffset;
            LOG.info("Read last commit offset from zookeeper: " + _committedTo + "; old topology_id: " + jsonTopologyId + " - new topology_id: " + topologyInstanceId );
        }

        LOG.info("Starting " + _partition + " from offset " + _committedTo);
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

    private void fill() {
        long start = System.nanoTime();
        ByteBufferMessageSet msgs = KafkaUtils.fetchMessages(_spoutConfig, _consumer, _partition, _emittedToOffset);
        long end = System.nanoTime();
        long millis = (end - start) / 1000000;
        _fetchAPILatencyMax.update(millis);
        _fetchAPILatencyMean.update(millis);
        _fetchAPICallCount.incr();
        int numMessages = countMessages(msgs);
        _fetchAPIMessageCount.incrBy(numMessages);

        if (numMessages > 0) {
            LOG.info("Fetched " + numMessages + " messages from: " + _partition);
        }
        for (MessageAndOffset msg : msgs) {
            _pending.add(_emittedToOffset);
            _waitingToEmit.add(new MessageAndRealOffset(msg.message(), _emittedToOffset));
            _emittedToOffset = msg.nextOffset();
        }
        if (numMessages > 0) {
            LOG.info("Added " + numMessages + " messages from: " + _partition + " to internal buffers");
        }
    }

    private int countMessages(ByteBufferMessageSet messageSet) {
        int counter = 0;
        for (MessageAndOffset messageAndOffset : messageSet) {
            counter = counter + 1;
        }
        return counter;
    }

    public void ack(Long offset) {
        _pending.remove(offset);
    }

    public void fail(Long offset) {
        //TODO: should it use in-memory ack set to skip anything that's been acked but not committed???
        // things might get crazy with lots of timeouts
        if (_emittedToOffset > offset) {
            _emittedToOffset = offset;
            _pending.tailSet(offset).clear();
        }
    }

    public void commit() {
        long lastCompletedOffset = lastCompletedOffset();
        if (lastCompletedOffset != lastCommittedOffset()) {
            LOG.info("Writing last completed offset (" + lastCompletedOffset + ") to ZK for " + _partition + " for topology: " + _topologyInstanceId);
            Map<Object, Object> data = ImmutableMap.builder()
                    .put("topology", ImmutableMap.of("id", _topologyInstanceId,
                            "name", _stormConf.get(Config.TOPOLOGY_NAME)))
                    .put("offset", lastCompletedOffset)
                    .put("partition", _partition.partition)
                    .put("broker", ImmutableMap.of("host", _partition.host.host,
                            "port", _partition.host.port))
                    .put("topic", _spoutConfig.topic).build();
            _state.writeJSON(committedPath(), data);
            _committedTo = lastCompletedOffset;
            LOG.info("Wrote last completed offset (" + lastCompletedOffset + ") to ZK for " + _partition + " for topology: " + _topologyInstanceId);
        } else {
            LOG.info("No new offset for " + _partition + " for topology: " + _topologyInstanceId);
        }
    }

    private String committedPath() {
        return _spoutConfig.zkRoot + "/" + _spoutConfig.id + "/" + _partition.getId();
    }

    public long queryPartitionOffsetLatestTime() {
        return KafkaUtils.getOffset(_consumer, _spoutConfig.topic, _partition.partition,
                OffsetRequest.LatestTime());
    }

    public long lastCommittedOffset() {
        return _committedTo;
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
}
