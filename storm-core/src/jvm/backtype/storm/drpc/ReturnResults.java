package backtype.storm.drpc;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;
import backtype.storm.utils.Utils;
import com.google.common.net.HostAndPort;
import org.apache.thrift7.TException;
import org.json.simple.JSONValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;


public class ReturnResults extends BaseRichBolt {
    public static final Logger LOG = LoggerFactory.getLogger(ReturnResults.class);

    protected OutputCollector _collector;
    protected DRPCInvocationsFactory _factory;

    protected Map<HostAndPort, DRPCInvocations> _clients = new HashMap<HostAndPort, DRPCInvocations>();

    @Deprecated
    public ReturnResults() {
    }

    public ReturnResults(DRPCInvocationsFactory factory) {
        this._factory = factory;
    }

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        _collector = collector;
    }

    @Override
    public void execute(Tuple input) {
        String result = (String) input.getValue(0);
        String returnInfo = (String) input.getValue(1);
        if (returnInfo != null) {
            Map retMap = (Map) JSONValue.parse(returnInfo);
            final String host = (String) retMap.get("host");
            final int port = Utils.getInt(retMap.get("port"));
            String id = (String) retMap.get("id");
            DRPCInvocations client;
            HostAndPort server = HostAndPort.fromParts(host, port);

            if (!_clients.containsKey(server)) {
                _clients.put(server, _factory.getClientForServer(server));
            }
            client = _clients.get(server);

            try {
                client.result(id, result);
                _collector.ack(input);
            } catch (TException e) {
                LOG.error("Failed to return results to DRPC server", e);
                _collector.fail(input);
            }
        }
    }

    @Override
    public void cleanup() {
        for (DRPCInvocations c : _clients.values()) {
            c.close();
        }
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
    }
}
