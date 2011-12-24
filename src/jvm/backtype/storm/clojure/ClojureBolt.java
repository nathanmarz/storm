package backtype.storm.clojure;

import backtype.storm.coordination.CoordinatedBolt.FinishedCallback;
import backtype.storm.coordination.FinishedTuple;
import backtype.storm.generated.StreamInfo;
import backtype.storm.task.IBolt;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.utils.Utils;
import clojure.lang.IFn;
import clojure.lang.RT;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;


public class ClojureBolt implements IRichBolt, FinishedCallback {
    Map<String, StreamInfo> _fields;
    String _namespace;
    String _fnName;
    List<Object> _params;
    
    IBolt _bolt;
    
    public ClojureBolt(String namespace, String fnName, List<Object> params, Map<String, StreamInfo> fields) {
        _namespace = namespace;
        _fnName = fnName;
        _params = params;
        _fields = fields;
    }

    @Override
    public void prepare(final Map stormConf, final TopologyContext context, final OutputCollector collector) {
        IFn hof = Utils.loadClojureFn(_namespace, _fnName);
        try {
            IFn preparer = (IFn) hof.applyTo(RT.seq(_params));
            List<Object> args = new ArrayList<Object>() {{
                add(stormConf);
                add(context);
                add(collector);
            }};
            
            _bolt = (IBolt) preparer.applyTo(RT.seq(args));
            //this is kind of unnecessary for clojure
            try {
                _bolt.prepare(stormConf, context, collector);
            } catch(AbstractMethodError ame) {
                
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void execute(Tuple input) {
        _bolt.execute(input);
    }

    @Override
    public void cleanup() {
            try {
                _bolt.cleanup();
            } catch(AbstractMethodError ame) {
                
            }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        for(String stream: _fields.keySet()) {
            StreamInfo info = _fields.get(stream);
            declarer.declareStream(stream, info.is_direct(), new Fields(info.get_output_fields()));
        }
    }

    @Override
    public void finishedId(FinishedTuple id) {
        if(_bolt instanceof FinishedCallback) {
            ((FinishedCallback) _bolt).finishedId(id);
        }
    }
}
