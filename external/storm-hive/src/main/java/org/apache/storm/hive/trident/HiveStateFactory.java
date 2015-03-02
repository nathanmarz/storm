package org.apache.storm.hive.trident;

import backtype.storm.task.IMetricsContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import storm.trident.state.State;
import storm.trident.state.StateFactory;
import org.apache.storm.hive.common.HiveOptions;

import java.util.Map;


public class HiveStateFactory implements StateFactory {
    private static final Logger LOG = LoggerFactory.getLogger(HiveStateFactory.class);
    private HiveOptions options;

    public HiveStateFactory(){}

    public HiveStateFactory withOptions(HiveOptions options){
        this.options = options;
        return this;
    }

    @Override
    public State makeState(Map conf, IMetricsContext metrics, int partitionIndex, int numPartitions) {
        LOG.info("makeState(partitonIndex={}, numpartitions={}", partitionIndex, numPartitions);
        HiveState state = new HiveState(this.options);
        state.prepare(conf, metrics, partitionIndex, numPartitions);
        return state;
    }
}
