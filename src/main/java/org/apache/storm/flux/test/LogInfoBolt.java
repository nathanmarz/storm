package org.apache.storm.flux.test;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.IBasicBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LogInfoBolt extends BaseBasicBolt {
    private static final Logger LOG = LoggerFactory.getLogger(LogInfoBolt.class);


    @Override
    public void execute(Tuple tuple, BasicOutputCollector basicOutputCollector) {
       LOG.info("{}", tuple);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

    }
}
