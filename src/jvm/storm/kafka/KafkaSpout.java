package storm.kafka;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.transactional.state.TransactionalState;
import backtype.storm.utils.Utils;
import java.util.ArrayList;
import kafka.javaapi.consumer.SimpleConsumer;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.SortedSet;
import java.util.TreeSet;
import kafka.javaapi.message.ByteBufferMessageSet;
import kafka.message.MessageAndOffset;
import kafka.api.FetchRequest;


public class KafkaSpout extends BaseRichSpout {
    static class KafkaMessageId {
        public int partition;
        public long offset;
        
        public KafkaMessageId(int partition, long offset) {
            this.partition = partition;
            this.offset = offset;
        }
    }
    
    protected class PartitionManager {
        Long _emittedToOffset;
        SortedSet<Long> _pending = new TreeSet<Long>();
        Long _committedTo;
        int _partition;
        LinkedList<MessageAndOffset> _waitingToEmit = new LinkedList<MessageAndOffset>();
        
        public PartitionManager(int partition) {
            _partition = partition;
            _committedTo = (Long) _state.getData(committedPath());
            if(_committedTo==null) _committedTo = 0L;
            _emittedToOffset = _committedTo;            
        }
        
        public boolean next() {
            if(_waitingToEmit.isEmpty()) fill();
            MessageAndOffset toEmit = _waitingToEmit.pollFirst();
            if(toEmit==null) return false;
            List<Object> tup = _kafkaConfig.scheme.deserialize(Utils.toByteArray(toEmit.message().payload()));
            _collector.emit(tup, new KafkaMessageId(_partition, actualOffset(toEmit)));
            return true;
        }
        
        private void fill() {
            SimpleConsumer consumer = _partitions.getConsumer(_partition);
            ByteBufferMessageSet msgs = consumer.fetch(
                    new FetchRequest(
                        _kafkaConfig.topic,
                        _partitions.getHostPartition(_partition),
                        _emittedToOffset,
                        _kafkaConfig.fetchSizeBytes));
            for(MessageAndOffset msg: msgs) {
                _pending.add(actualOffset(msg));
                _waitingToEmit.add(msg);
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
                _state.setData(committedPath(), committedTo);
                _committedTo = committedTo;
            }
        }
        
        private long actualOffset(MessageAndOffset msg) {
            return msg.offset() - msg.message().serializedSize();

        }
        
        private String committedPath() {
            return _spoutConfig.id + "/" + _partition;
        }
    }
    
    KafkaConfig _kafkaConfig;
    SpoutConfig _spoutConfig;
    SpoutOutputCollector _collector;
    TransactionalState _state;
    KafkaPartitionConnections _partitions;
    Map<Integer, PartitionManager> _managers = new HashMap<Integer, PartitionManager>();

    long _lastUpdateMs = 0;
    
    int _currPartitionIndex = 0;
    List<Integer> _managedPartitions = new ArrayList<Integer>();
        
    public KafkaSpout(SpoutConfig spoutConf, KafkaConfig kafkaConfig) {
        _kafkaConfig = kafkaConfig;
        _spoutConfig = spoutConf;
    }
    
    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        _collector = collector;
        Map stateConf = new HashMap(conf);
        
        List<String> zkServers = _spoutConfig.zkServers;
        if(zkServers==null) zkServers = (List<String>) conf.get(Config.STORM_ZOOKEEPER_SERVERS);

        Integer zkPort = _spoutConfig.zkPort;
        if(zkPort==null) zkPort = ((Number) conf.get(Config.STORM_ZOOKEEPER_PORT)).intValue();
        
        String zkRoot = _spoutConfig.zkRoot;
        
        stateConf.put(Config.TRANSACTIONAL_ZOOKEEPER_SERVERS, zkServers);
        stateConf.put(Config.TRANSACTIONAL_ZOOKEEPER_PORT, zkPort);
        stateConf.put(Config.TRANSACTIONAL_ZOOKEEPER_ROOT, zkRoot);
        
        // using TransactionalState like this is a hack
        _state = TransactionalState.newUserState(stateConf, _spoutConfig.id, null);
        _partitions = new KafkaPartitionConnections(_kafkaConfig);

        int totalPartitions = _kafkaConfig.partitionsPerHost * _kafkaConfig.hosts.size();
        int numTasks = context.getComponentTasks(context.getThisComponentId()).size();
        for(int p = context.getThisTaskIndex(); p < totalPartitions; p+=numTasks) {
            _managedPartitions.add(p);
            _managers.put(p, new PartitionManager(p));
        }

    }

    @Override
    public void nextTuple() {
        
        for(int i=0; i<_managedPartitions.size(); i++) {
            int partition = _managedPartitions.get(_currPartitionIndex);
            _currPartitionIndex = (_currPartitionIndex + 1) % _managedPartitions.size();
            if(_managers.get(partition).next()) break;
        }
        
        long now = System.currentTimeMillis();
        if((now - _lastUpdateMs) > _spoutConfig.stateUpdateIntervalMs) {
            commit();
        }

    }

    @Override
    public void ack(Object msgId) {
        KafkaMessageId id = (KafkaMessageId) msgId;
        _managers.get(id.partition).ack(id.offset);
    }

    @Override
    public void fail(Object msgId) {
        KafkaMessageId id = (KafkaMessageId) msgId;
        _managers.get(id.partition).fail(id.offset);
    }

    @Override
    public void deactivate() {
        commit();
    }    
    
    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(_kafkaConfig.scheme.getOutputFields());
    }
    
    private void commit() {
        _lastUpdateMs = System.currentTimeMillis();
        for(PartitionManager manager: _managers.values()) {
            manager.commit();
        }        
    }
    
    
    public static void main(String[] args) {
        TopologyBuilder builder = new TopologyBuilder();
        List<String> hosts = new ArrayList<String>();
        hosts.add("localhost");
        KafkaConfig kafkaConf = new KafkaConfig(hosts, 3, "nathan");
        kafkaConf.scheme = new StringScheme();
        SpoutConfig spoutConf = new SpoutConfig("/kafka", "id");
        spoutConf.zkServers = new ArrayList<String>() {{
           add("localhost"); 
        }};
        spoutConf.zkPort = 2181;
        
        builder.setSpout("spout",
                new KafkaSpout(spoutConf, kafkaConf));
        
        Config conf = new Config();
        conf.setDebug(true);
        
        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("kafka-test", conf, builder.createTopology());
        
        Utils.sleep(600000);
    }
}
