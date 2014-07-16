package storm.kafka.trident;


import backtype.storm.task.IMetricsContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import storm.trident.state.State;
import storm.trident.state.StateFactory;

import java.util.Map;

public class TridentKafkaStateFactory implements StateFactory {

    private static final Logger LOG = LoggerFactory.getLogger(TridentKafkaStateFactory.class);

    public TridentKafkaStateFactory(){}

    @Override
    public State makeState(Map conf, IMetricsContext metrics, int partitionIndex, int numPartitions) {
        LOG.info("makeState(partitonIndex={}, numpartitions={}", partitionIndex, numPartitions);
        TridentKafkaState state = new TridentKafkaState();
        state.prepare(conf);
        return state;
    }
}
