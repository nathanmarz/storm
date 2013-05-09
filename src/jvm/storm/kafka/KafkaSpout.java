package storm.kafka;

import java.util.*;

import backtype.storm.metric.api.*;
import backtype.storm.task.IMetricsContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.Config;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import kafka.message.Message;
import storm.kafka.PartitionManager.KafkaMessageId;
import storm.kafka.trident.KafkaUtils;

// TODO: need to add blacklisting
// TODO: need to make a best effort to not re-emit messages if don't have to
public class KafkaSpout extends BaseRichSpout {
    public static class MessageAndRealOffset {
        public Message msg;
        public long offset;

        public MessageAndRealOffset(Message msg, long offset) {
            this.msg = msg;
            this.offset = offset;
        }
    }

    static enum EmitState {
        EMITTED_MORE_LEFT,
        EMITTED_END,
        NO_EMITTED
    }

    public static final Logger LOG = LoggerFactory.getLogger(KafkaSpout.class);

    String _uuid = UUID.randomUUID().toString();
    SpoutConfig _spoutConfig;
    SpoutOutputCollector _collector;
    PartitionCoordinator _coordinator;
    DynamicPartitionConnections _connections;
    ZkState _state;

    long _lastUpdateMs = 0;

    int _currPartitionIndex = 0;

    public KafkaSpout(SpoutConfig spoutConf) {
        _spoutConfig = spoutConf;
    }

    @Override
    public void open(Map conf, final TopologyContext context, final SpoutOutputCollector collector) {
        _collector = collector;

	Map stateConf = new HashMap(conf);
        List<String> zkServers = _spoutConfig.zkServers;
        if(zkServers==null) zkServers = (List<String>) conf.get(Config.STORM_ZOOKEEPER_SERVERS);
        Integer zkPort = _spoutConfig.zkPort;
        if(zkPort==null) zkPort = ((Number) conf.get(Config.STORM_ZOOKEEPER_PORT)).intValue();
        stateConf.put(Config.TRANSACTIONAL_ZOOKEEPER_SERVERS, zkServers);
        stateConf.put(Config.TRANSACTIONAL_ZOOKEEPER_PORT, zkPort);
        stateConf.put(Config.TRANSACTIONAL_ZOOKEEPER_ROOT, _spoutConfig.zkRoot);
	_state = new ZkState(stateConf);

        _connections = new DynamicPartitionConnections(_spoutConfig);

        // using TransactionalState like this is a hack
        int totalTasks = context.getComponentTasks(context.getThisComponentId()).size();
        if(_spoutConfig.hosts instanceof KafkaConfig.StaticHosts) {
            _coordinator = new StaticCoordinator(_connections, conf, _spoutConfig, _state, context.getThisTaskIndex(), totalTasks, _uuid);
        } else {
            _coordinator = new ZkCoordinator(_connections, conf, _spoutConfig, _state, context.getThisTaskIndex(), totalTasks, _uuid);
        }

        context.registerMetric("kafkaOffset", new IMetric() {
            KafkaUtils.KafkaOffsetMetric _kafkaOffsetMetric = new KafkaUtils.KafkaOffsetMetric(_spoutConfig.topic, _connections);
            @Override
            public Object getValueAndReset() {
                List<PartitionManager> pms = _coordinator.getMyManagedPartitions();
                Set<GlobalPartitionId> latestPartitions = new HashSet();
                for(PartitionManager pm : pms) { latestPartitions.add(pm.getPartition()); }
                _kafkaOffsetMetric.refreshPartitions(latestPartitions);
                for(PartitionManager pm : pms) {
                    _kafkaOffsetMetric.setLatestEmittedOffset(pm.getPartition(), pm.lastCompletedOffset());
                }
                return _kafkaOffsetMetric.getValueAndReset();
            }
        }, 60);

        context.registerMetric("kafkaPartition", new IMetric() {
            @Override
            public Object getValueAndReset() {
                List<PartitionManager> pms = _coordinator.getMyManagedPartitions();
                Map concatMetricsDataMaps = new HashMap();
                for(PartitionManager pm : pms) {
                    concatMetricsDataMaps.putAll(pm.getMetricsDataMap());
                }
                return concatMetricsDataMaps;
            }
        }, 60);
    }

    @Override
    public void close() {
	_state.close();
    }

    @Override
    public void nextTuple() {
        List<PartitionManager> managers = _coordinator.getMyManagedPartitions();
        for(int i=0; i<managers.size(); i++) {
            
            // in case the number of managers decreased
            _currPartitionIndex = _currPartitionIndex % managers.size();
            EmitState state = managers.get(_currPartitionIndex).next(_collector);
            if(state!=EmitState.EMITTED_MORE_LEFT) {
                _currPartitionIndex = (_currPartitionIndex + 1) % managers.size();
            }
            if(state!=EmitState.NO_EMITTED) {
                break;
            }
        }

        long now = System.currentTimeMillis();
        if((now - _lastUpdateMs) > _spoutConfig.stateUpdateIntervalMs) {
            commit();
        }
    }

    @Override
    public void ack(Object msgId) {
        KafkaMessageId id = (KafkaMessageId) msgId;
        PartitionManager m = _coordinator.getManager(id.partition);
        if(m!=null) {
            m.ack(id.offset);
        }                
    }

    @Override
    public void fail(Object msgId) {
        KafkaMessageId id = (KafkaMessageId) msgId;
        PartitionManager m = _coordinator.getManager(id.partition);
        if(m!=null) {
            m.fail(id.offset);
        } 
    }

    @Override
    public void deactivate() {
        commit();
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(_spoutConfig.scheme.getOutputFields());
    }

    private void commit() {
        _lastUpdateMs = System.currentTimeMillis();
        for(PartitionManager manager: _coordinator.getMyManagedPartitions()) {
            manager.commit();
        }
    }

    public static void main(String[] args) {
//        TopologyBuilder builder = new TopologyBuilder();
//        List<String> hosts = new ArrayList<String>();
//        hosts.add("localhost");
//        SpoutConfig spoutConf = SpoutConfig.fromHostStrings(hosts, 8, "clicks", "/kafkastorm", "id");
//        spoutConf.scheme = new SchemeAsMultiScheme(new StringScheme());
//        spoutConf.forceStartOffsetTime(-2);
//
// //       spoutConf.zkServers = new ArrayList<String>() {{
// //          add("localhost");
// //       }};
// //       spoutConf.zkPort = 2181;
//
//        builder.setSpout("spout", new KafkaSpout(spoutConf), 3);
//
//        Config conf = new Config();
//        //conf.setDebug(true);
//
//        LocalCluster cluster = new LocalCluster();
//        cluster.submitTopology("kafka-test", conf, builder.createTopology());
//
//        Utils.sleep(600000);
    }
}
