package org.apache.storm.hdfs.trident;

import backtype.storm.task.IMetricsContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import storm.trident.state.State;
import storm.trident.state.StateFactory;

import java.util.Map;

public class HdfsStateFactory implements StateFactory {
    private static final Logger LOG = LoggerFactory.getLogger(HdfsStateFactory.class);
    private HdfsState.Options options;

    public HdfsStateFactory(){}

    public HdfsStateFactory withOptions(HdfsState.Options options){
        this.options = options;
        return this;
    }

    @Override
    public State makeState(Map conf, IMetricsContext metrics, int partitionIndex, int numPartitions) {
        LOG.info("makeState(partitonIndex={}, numpartitions={}", partitionIndex, numPartitions);
        HdfsState state = new HdfsState(this.options);
        state.prepare(conf, metrics, partitionIndex, numPartitions);
        return state;
    }
}
