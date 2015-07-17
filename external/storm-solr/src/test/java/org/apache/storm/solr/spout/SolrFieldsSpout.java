package org.apache.storm.solr.spout;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import com.google.common.collect.Lists;
import org.apache.storm.solr.util.TestUtil;

import java.util.List;
import java.util.Map;
import java.util.Random;

/**
 * Created by hlouro on 7/24/15.
 */
public class SolrFieldsSpout extends BaseRichSpout {
    private SpoutOutputCollector collector;
    public static final List<Values> listValues = Lists.newArrayList(
            getValues("1"), getValues("2"), getValues("3"));

    private static Values getValues(String suf) {
        String suffix = "_hmcl_fields_test_val_" + suf;
        return new Values(
                "id" + suffix,
                TestUtil.getDate(),
                "dc_title" + suffix,
                "dynamic_field" + suffix + "_txt",      // to match dynamic fields of the form "*_txt"
                "non_matching_field" + suffix);         // this field won't be indexed by solr
    }

    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        this.collector = collector;
    }

    @Override
    public void nextTuple() {
        final Random rand = new Random();
        final Values values = listValues.get(rand.nextInt(listValues.size()));
        collector.emit(values);
        Thread.yield();
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(getOutputFields());
    }

    public Fields getOutputFields() {
        return new Fields("id","date","dc_title","dynamic_field_txt","non_matching_field");
    }

    @Override
    public void close() {
        super.close();
    }
}
