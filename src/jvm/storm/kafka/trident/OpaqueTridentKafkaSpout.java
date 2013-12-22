package storm.kafka.trident;

import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.Fields;
import storm.kafka.Partition;
import storm.trident.spout.IOpaquePartitionedTridentSpout;

import java.util.Map;
import java.util.UUID;


public class OpaqueTridentKafkaSpout implements IOpaquePartitionedTridentSpout<GlobalPartitionInformation, Partition, Map> {


    TridentKafkaConfig _config;
    String _topologyInstanceId = UUID.randomUUID().toString();

    public OpaqueTridentKafkaSpout(TridentKafkaConfig config) {
        _config = config;
    }

    @Override
    public IOpaquePartitionedTridentSpout.Emitter<GlobalPartitionInformation, Partition, Map> getEmitter(Map conf, TopologyContext context) {
        return new TridentKafkaEmitter(conf, context, _config, _topologyInstanceId).asOpaqueEmitter();
    }

    @Override
    public IOpaquePartitionedTridentSpout.Coordinator getCoordinator(Map conf, TopologyContext tc) {
        return new storm.kafka.trident.Coordinator(conf, _config);
    }

    @Override
    public Fields getOutputFields() {
        return _config.scheme.getOutputFields();
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }

}
