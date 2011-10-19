package backtype.storm.drpc;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;
import backtype.storm.utils.DRPCClient;
import java.util.Map;
import org.apache.thrift7.TException;
import org.json.simple.JSONValue;


public class ReturnResults implements IRichBolt {

    OutputCollector _collector;

    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        _collector = collector;
    }

    public void execute(Tuple input) {
        String result = (String) input.getValue(0);
        String returnInfo = (String) input.getValue(1);
        if(returnInfo!=null) {
            Map retMap = (Map) JSONValue.parse(returnInfo);
            String ip = (String) retMap.get("ip");
            Long port = (Long) retMap.get("port");
            String id = (String) retMap.get("id");
            try {
                DRPCClient client = new DRPCClient(ip, (int) port.longValue());
                client.result(id, result);
                client.close();
                _collector.ack(input);
            } catch(TException e) {
                _collector.fail(input);
            }
        }
    }

    public void cleanup() {

    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
    }

}
