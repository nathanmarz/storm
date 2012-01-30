package backtype.storm.drpc;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import java.util.Map;
import java.util.Random;
import backtype.storm.utils.Utils;


public class PrepareRequest extends BaseBasicBolt {
    public static final String ARGS_STREAM = Utils.DEFAULT_STREAM_ID;
    public static final String RETURN_STREAM = "ret";
    public static final String ID_STREAM = "id";

    Random rand;

    @Override
    public void prepare(Map map, TopologyContext context) {
        rand = new Random();
    }

    @Override
    public void execute(Tuple tuple, BasicOutputCollector collector) {
        String args = tuple.getString(0);
        String returnInfo = tuple.getString(1);
        long requestId = rand.nextLong();
        collector.emit(ARGS_STREAM, new Values(requestId, args));
        collector.emit(RETURN_STREAM, new Values(requestId, returnInfo));
        collector.emit(ID_STREAM, new Values(requestId));
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declareStream(ARGS_STREAM, new Fields("request", "args"));
        declarer.declareStream(RETURN_STREAM, new Fields("request", "return"));
        declarer.declareStream(ID_STREAM, new Fields("request"));
    }
}
