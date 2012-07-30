package storm.kafka;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.utils.Utils;
import java.util.*;
import kafka.api.FetchRequest;
import kafka.javaapi.consumer.SimpleConsumer;
import kafka.javaapi.message.ByteBufferMessageSet;
import kafka.message.MessageAndOffset;
import org.apache.log4j.Logger;
import storm.kafka.KafkaSpout.EmitState;
import storm.kafka.KafkaSpout.MessageAndRealOffset;

public class PartitionManager {  
    public static final Logger LOG = Logger.getLogger(PartitionManager.class);

    static class KafkaMessageId {
        public GlobalPartitionId partition;
        public long offset;

        public KafkaMessageId(GlobalPartitionId partition, long offset) {
            this.partition = partition;
            this.offset = offset;
        }
    }    
    
    Long _emittedToOffset;
    SortedSet<Long> _pending = new TreeSet<Long>();
    Long _committedTo;
    LinkedList<MessageAndRealOffset> _waitingToEmit = new LinkedList<MessageAndRealOffset>();
    GlobalPartitionId _partition;
    SpoutConfig _spoutConfig;
    String _topologyInstanceId;
    SimpleConsumer _consumer;
    DynamicPartitionConnections _connections;
    KafkaSpoutState _state;
    
    public PartitionManager(DynamicPartitionConnections connections, String topologyInstanceId, KafkaSpoutState state, SpoutConfig spoutConfig, GlobalPartitionId id) {
        _partition = id;
        _connections = connections;
        _spoutConfig = spoutConfig;
        _topologyInstanceId = topologyInstanceId;
        _consumer = connections.register(id.host, id.partition);
	_state = state;

	Map<String, Object> st = _state.readData(committedPath());
        if(st==null || (!topologyInstanceId.equals((String)st.get("id")) && spoutConfig.forceFromStart)) {
            _committedTo = _consumer.getOffsetsBefore(spoutConfig.topic, id.partition, spoutConfig.startOffsetTime, 1)[0];
        } else {
            _committedTo = (Long)st.get("offset");
	    LOG.info("Read last commit offset from zookeeper: " + _committedTo);
        }
        LOG.info("Starting Kafka " + _consumer.host() + ":" + id.partition + " from offset " + _committedTo);
        _emittedToOffset = _committedTo;
    }

    //returns false if it's reached the end of current batch
    public EmitState next(SpoutOutputCollector collector) {
        if(_waitingToEmit.isEmpty()) fill();
        while(true) {
            MessageAndRealOffset toEmit = _waitingToEmit.pollFirst();
            if(toEmit==null) {
                return EmitState.NO_EMITTED;
            }
            List<Object> tup = _spoutConfig.scheme.deserialize(Utils.toByteArray(toEmit.msg.payload()));
            if(tup!=null) {
                collector.emit(tup, new KafkaMessageId(_partition, toEmit.offset));
                break;
            } else {
                ack(toEmit.offset);
            }
        }
        if(!_waitingToEmit.isEmpty()) {
            return EmitState.EMITTED_MORE_LEFT;
        } else {
            return EmitState.EMITTED_END;
        }
    }

    private void fill() {
        LOG.info("Fetching from Kafka: " + _consumer.host() + ":" + _partition.partition + " from offset " + _emittedToOffset);
        ByteBufferMessageSet msgs = _consumer.fetch(
                new FetchRequest(
                    _spoutConfig.topic,
                    _partition.partition,
                    _emittedToOffset,
                    _spoutConfig.fetchSizeBytes));
        LOG.info("Fetched " + msgs.underlying().size() + " messages from Kafka: " + _consumer.host() + ":" + _partition.partition);
        for(MessageAndOffset msg: msgs) {
            _pending.add(_emittedToOffset);
            _waitingToEmit.add(new MessageAndRealOffset(msg.message(), _emittedToOffset));
            _emittedToOffset = msg.offset();
        }
        LOG.info("Added " + msgs.underlying().size() + " messages from Kafka: " + _consumer.host() + ":" + _partition.partition + " to internal buffers");
    }

    public void ack(Long offset) {
        _pending.remove(offset);
    }

    public void fail(Long offset) {
        //TODO: should it use in-memory ack set to skip anything that's been acked but not committed???
        // things might get crazy with lots of timeouts
        if(_emittedToOffset > offset) {
            _emittedToOffset = offset;
            _pending.tailSet(offset).clear();
        }
    }

    public void commit() {
        LOG.info("Committing offset for " + _partition);
        long committedTo;
        if(_pending.isEmpty()) {
            committedTo = _emittedToOffset;
        } else {
            committedTo = _pending.first();
        }
        if(committedTo!=_committedTo) {
            LOG.info("Writing committed offset to ZK: " + committedTo);

	    Map<String, Object> data = new LinkedHashMap<String, Object>();
	    data.put("id", _topologyInstanceId);
	    data.put("offset", committedTo);
	    _state.writeData(committedPath(), data);

            LOG.info("Wrote committed offset to ZK: " + committedTo);
            _committedTo = committedTo;
        }
        LOG.info("Comitted offset for " + _partition);
    }

    private String committedPath() {
        return _spoutConfig.zkRoot + "/" + _spoutConfig.id + "/" + _partition;
    }
    
    public void close() {
        _connections.unregister(_partition.host, _partition.partition);
    }
}

