package storm.kafka;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.transactional.state.TransactionalState;
import backtype.storm.utils.Utils;
import java.util.LinkedList;
import java.util.List;
import java.util.SortedSet;
import java.util.TreeSet;
import kafka.api.FetchRequest;
import kafka.javaapi.consumer.SimpleConsumer;
import kafka.javaapi.message.ByteBufferMessageSet;
import kafka.message.MessageAndOffset;
import org.apache.log4j.Logger;
import storm.kafka.KafkaSpout.EmitState;
import storm.kafka.KafkaSpout.MessageAndRealOffset;
import storm.kafka.KafkaSpout.ZooMeta;

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
    TransactionalState _state;
    GlobalPartitionId _partition;
    SpoutConfig _spoutConfig;
    String _topologyInstanceId;
    SimpleConsumer _consumer;
    DynamicPartitionConnections _connections;
    
    public PartitionManager(DynamicPartitionConnections connections, String topologyInstanceId, SpoutConfig spoutConfig, TransactionalState state, GlobalPartitionId id) {
        _state = state;
        _partition = id;
        _connections = connections;
        _spoutConfig = spoutConfig;
        _topologyInstanceId = topologyInstanceId;
        ZooMeta zooMeta = (ZooMeta) _state.getData(committedPath());
        _consumer = connections.register(id.host, id.partition); 

        //the id stuff makes sure the spout doesn't reset the offset if it restarts
        if(zooMeta==null || (!topologyInstanceId.equals(zooMeta.id) && spoutConfig.forceFromStart)) {
            _committedTo = _consumer.getOffsetsBefore(spoutConfig.topic, id.partition, spoutConfig.startOffsetTime, 1)[0];
        } else {
            _committedTo = zooMeta.offset;
        }
        LOG.info("Starting Kafka " + _consumer.host() + ":" + id.partition + " from offset " + _committedTo);
        _emittedToOffset = _committedTo;
    }

    //returns false if it's reached the end of current batch
    public EmitState next(SpoutOutputCollector collector) {
        if(_waitingToEmit.isEmpty()) fill();
        MessageAndRealOffset toEmit = _waitingToEmit.pollFirst();
        if(toEmit==null) {
            return EmitState.NO_EMITTED;
        }
        List<Object> tup = _spoutConfig.scheme.deserialize(Utils.toByteArray(toEmit.msg.payload()));
        collector.emit(tup, new KafkaMessageId(_partition, toEmit.offset));
        if(_waitingToEmit.size()>0) {
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
        long committedTo;
        if(_pending.isEmpty()) {
            committedTo = _emittedToOffset;
        } else {
            committedTo = _pending.first();
        }
        if(committedTo!=_committedTo) {
            _state.setData(committedPath(), new ZooMeta(_topologyInstanceId, committedTo));
            _committedTo = committedTo;
        }
    }

    private String committedPath() {
        return _spoutConfig.id + "/" + _partition;
    }
    
    public void close() {
        _connections.unregister(_partition.host, _partition.partition);
    }
}