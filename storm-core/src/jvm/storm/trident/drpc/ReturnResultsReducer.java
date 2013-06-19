package storm.trident.drpc;

import backtype.storm.drpc.DRPCInvocations;
import backtype.storm.drpc.DRPCInvocationsFactory;
import backtype.storm.utils.Utils;
import com.google.common.net.HostAndPort;
import org.apache.commons.lang.builder.ToStringBuilder;
import org.apache.thrift7.TException;
import org.json.simple.JSONValue;
import storm.trident.drpc.ReturnResultsReducer.ReturnResultsState;
import storm.trident.operation.MultiReducer;
import storm.trident.operation.TridentCollector;
import storm.trident.operation.TridentMultiReducerContext;
import storm.trident.tuple.TridentTuple;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


public class ReturnResultsReducer implements MultiReducer<ReturnResultsState> {
    public static class ReturnResultsState {
        List<TridentTuple> results = new ArrayList<TridentTuple>();
        String returnInfo;

        @Override
        public String toString() {
            return ToStringBuilder.reflectionToString(this);
        }
    }

    protected DRPCInvocationsFactory _factory;
    Map<HostAndPort, DRPCInvocations> _clients = new HashMap<HostAndPort, DRPCInvocations>();

    public ReturnResultsReducer() {
    }

    public ReturnResultsReducer(DRPCInvocationsFactory factory) {
        this._factory = factory;
    }

    @Override
    public void prepare(Map stormConf, TridentMultiReducerContext context) {
    }

    @Override
    public ReturnResultsState init(TridentCollector collector) {
        return new ReturnResultsState();
    }

    @Override
    public void execute(ReturnResultsState state, int streamIndex, TridentTuple input, TridentCollector collector) {
        if (streamIndex == 0) {
            state.returnInfo = input.getString(0);
        } else {
            state.results.add(input);
        }
    }

    @Override
    public void complete(ReturnResultsState state, TridentCollector collector) {
        // only one of the multireducers will receive the tuples
        if (state.returnInfo != null) {
            String result = JSONValue.toJSONString(state.results);
            Map retMap = (Map) JSONValue.parse(state.returnInfo);
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
            } catch (TException e) {
                collector.reportError(e);
            }
        }
    }

    @Override
    public void cleanup() {
        for (DRPCInvocations c : _clients.values()) {
            c.close();
        }
    }

}
