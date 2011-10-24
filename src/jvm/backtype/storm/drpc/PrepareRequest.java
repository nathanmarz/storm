package backtype.storm.drpc;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.IBasicBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Random;


public class PrepareRequest implements IBasicBolt {
    public static final int ARGS_STREAM = 1;
    public static final int RETURN_STREAM = 2;

    Random rand;

    public void prepare(Map map, TopologyContext context) {
        rand = new Random();
    }

    public void execute(Tuple tuple, BasicOutputCollector collector) {
        String args = tuple.getString(0);
        String returnInfo = tuple.getString(1);
        long requestId = rand.nextLong();
        collector.emit(ARGS_STREAM, new Values(requestId, args));
        collector.emit(RETURN_STREAM, new Values(requestId, returnInfo));
    }

    public void cleanup() {
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declareStream(ARGS_STREAM, new Fields("request", "args"));
        declarer.declareStream(RETURN_STREAM, new Fields("request", "return"));
    }


}
