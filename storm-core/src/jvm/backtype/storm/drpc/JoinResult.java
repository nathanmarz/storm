package backtype.storm.drpc;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class JoinResult extends BaseRichBolt {
    public static Logger LOG = LoggerFactory.getLogger(JoinResult.class);

    String returnComponent;
    Map<Object, Tuple> returns = new HashMap<Object, Tuple>();
    Map<Object, Tuple> results = new HashMap<Object, Tuple>();
    OutputCollector _collector;

    public JoinResult(String returnComponent) {
        this.returnComponent = returnComponent;
    }
 
    public void prepare(Map map, TopologyContext context, OutputCollector collector) {
        _collector = collector;
    }

    public void execute(Tuple tuple) {
        Object requestId = tuple.getValue(0);
        if(tuple.getSourceComponent().equals(returnComponent)) {
            returns.put(requestId, tuple);
        } else {
            results.put(requestId, tuple);
        }

        if(returns.containsKey(requestId) && results.containsKey(requestId)) {
            Tuple result = results.remove(requestId);
            Tuple returner = returns.remove(requestId);
            LOG.debug(result.getValue(1).toString());
            List<Tuple> anchors = new ArrayList<Tuple>();
            anchors.add(result);
            anchors.add(returner);            
            _collector.emit(anchors, new Values(""+result.getValue(1), returner.getValue(1)));
            _collector.ack(result);
            _collector.ack(returner);
        }
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("result", "return-info"));
    }
}
