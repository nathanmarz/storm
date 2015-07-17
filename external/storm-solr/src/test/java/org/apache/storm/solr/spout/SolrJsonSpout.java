package org.apache.storm.solr.spout;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import com.google.common.collect.Lists;
import com.google.gson.Gson;
import org.junit.Test;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Random;

/**
 * Created by hlouro on 7/24/15.
 */
public class SolrJsonSpout extends BaseRichSpout {
    private SpoutOutputCollector collector;
    private static final List<Values> listValues = Lists.newArrayList(
            new Values((new JsonSchema("_hmcl_json_test_1")).toJson()),
            new Values((new JsonSchema("_hmcl_json_test_2")).toJson()),
            new Values((new JsonSchema("_hmcl_json_test_3")).toJson()),
            new Values(new JsonSchema("_hmcl_json_test_4")),
            new Values(new JsonSchema("_hmcl_json_test_5")));

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
        return new Fields("JSON");
    }

    @Override
    public void close() {   //TODO
        super.close();
    }

    public static class JsonSchema {
        private String id;
        private String date;
        private String dc_title;

        private static final Gson gson = new Gson();

        public JsonSchema(String suffix) {
            this.id = "id" + suffix;
            this.date = getDate();
            this.dc_title = "dc_title" + suffix;
        }

        public JsonSchema(String id, String date, String dc_title) {
            this.id = id;
            this.date = date;
            this.dc_title = dc_title;
        }

        // copy constructor
        public JsonSchema(JsonSchema jsonSchema) {
            this.id = jsonSchema.id;
            this.date = jsonSchema.date;
            this.dc_title = jsonSchema.dc_title;
        }

        public String toJson() {
            String json = gson.toJson(this);
            System.out.println(json);   // TODO log
            return json;
        }

        //TODO: clean this up
        public static JsonSchema fromJson(String jsonStr) {
            return new JsonSchema(gson.fromJson(jsonStr, JsonSchema.class));
        }

        private String getDate() {
            DateFormat df = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'");
            String date = df.format(new Date());
            System.out.println(date);
            return date;
        }
    }

    //TODO Delete
    @Test
    public void test() {
        SolrJsonSpout solrJsonSpout = new SolrJsonSpout();
        System.out.println("stop");
    }


}
