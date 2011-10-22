package backtype.storm.drpc;

import backtype.storm.Config;
import backtype.storm.ILocalDRPC;
import backtype.storm.generated.DistributedRPC;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;
import backtype.storm.utils.DRPCClient;
import backtype.storm.utils.ServiceRegistry;
import java.util.Map;
import org.apache.thrift7.TException;
import org.json.simple.JSONValue;


public class ReturnResults implements IRichBolt {

    OutputCollector _collector;
    boolean local;

    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        _collector = collector;
        local = stormConf.get(Config.STORM_CLUSTER_MODE).equals("local");
    }

    public void execute(Tuple input) {
        String result = (String) input.getValue(0);
        String returnInfo = (String) input.getValue(1);
        if(returnInfo!=null) {
            Map retMap = (Map) JSONValue.parse(returnInfo);
            String host = (String) retMap.get("host");
            int port = (int) ((Long) retMap.get("port")).longValue();
            String id = (String) retMap.get("id");
            DistributedRPC.Iface client;
            if(local) {
                client = (ILocalDRPC) ServiceRegistry.getService(host);
            } else {
                client = new DRPCClient(host, port);
            }
                
            try {
                client.result(id, result);
                if(!local) {
                    ((DRPCClient)client).close();                   
                }
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
