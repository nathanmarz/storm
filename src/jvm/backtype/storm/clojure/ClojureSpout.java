package backtype.storm.clojure;

import backtype.storm.generated.StreamInfo;
import backtype.storm.spout.ISpout;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.utils.Utils;
import clojure.lang.IFn;
import clojure.lang.RT;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class ClojureSpout implements IRichSpout {
    private boolean _isDistributed;
    Map<String, StreamInfo> _fields;
    String _namespace;
    String _fnName;
    List<Object> _params;
    
    ISpout _spout;
    
    public ClojureSpout(String namespace, String fnName, List<Object> params, Map<String, StreamInfo> fields, boolean isDistributed) {
        _isDistributed = isDistributed;
        _namespace = namespace;
        _fnName = fnName;
        _params = params;
        _fields = fields;
    }
    
    @Override
    public boolean isDistributed() {
        return _isDistributed;
    }

    @Override
    public void open(final Map conf, final TopologyContext context, final SpoutOutputCollector collector) {
        IFn hof = Utils.loadClojureFn(_namespace, _fnName);
        try {
            IFn preparer = (IFn) hof.applyTo(RT.seq(_params));
            List<Object> args = new ArrayList<Object>() {{
                add(conf);
                add(context);
                add(collector);
            }};
            
            _spout = (ISpout) preparer.applyTo(RT.seq(args));
            //this is kind of unnecessary for clojure
            try {
                _spout.open(conf, context, collector);
            } catch(AbstractMethodError ame) {
                
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void close() {
        try {
            _spout.close();
        } catch(AbstractMethodError ame) {
                
        }
    }

    @Override
    public void nextTuple() {
        try {
            _spout.nextTuple();
        } catch(AbstractMethodError ame) {
                
        }

    }

    @Override
    public void ack(Object msgId) {
        try {
            _spout.ack(msgId);
        } catch(AbstractMethodError ame) {
                
        }

    }

    @Override
    public void fail(Object msgId) {
        try {
            _spout.fail(msgId);
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
    
}
