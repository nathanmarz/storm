package backtype.storm.clojure;

import backtype.storm.generated.StreamInfo;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import clojure.lang.IFn;
import clojure.lang.Keyword;
import clojure.lang.RT;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;


public class ClojureBolt implements IRichBolt {
    Map<Integer, StreamInfo> _fields;
    String _namespace;
    String _fnName;
    List<Object> _args;
    
    IFn _execute;
    IFn _cleanup;
    
    OutputCollector _collector;
    
    
    public ClojureBolt(String namespace, String fnName, Map<Integer, StreamInfo> fields) {
        this(namespace, fnName, new ArrayList<Object>(), fields);
    }
    
    public ClojureBolt(String namespace, String fnName, List<Object> args, Map<Integer, StreamInfo> fields) {
        _namespace = namespace;
        _fnName = fnName;
        _args = args;
        _fields = fields;
    }

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        _collector = collector;
        try {
          clojure.lang.Compiler.eval(RT.readString("(require '" + _namespace + ")"));
        } catch (Exception e) {
          //if playing from the repl and defining functions, file won't exist
        }
        IFn hof = (IFn) RT.var(_namespace, _fnName).deref();
        try {
            Object fns = hof.applyTo(RT.seq(_args));
            if(fns instanceof Map) {
                Map fnMap = (Map) fns;
                IFn prepare = (IFn) fnMap.get(Keyword.intern("prepare"));
                if(prepare!=null)
                    prepare.invoke(stormConf, context, collector);
                _execute = (IFn) fnMap.get(Keyword.intern("execute"));
                _cleanup = (IFn) fnMap.get(Keyword.intern("cleanup"));
            } else {
                _execute = (IFn) fns;   
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void execute(Tuple input) {
        try {
            _execute.invoke(input, _collector);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }    
    }

    @Override
    public void cleanup() {
        if(_cleanup!=null) {
            try {
                _cleanup.invoke(_collector);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        for(Integer stream: _fields.keySet()) {
            StreamInfo info = _fields.get(stream);
            declarer.declareStream(stream, info.is_direct(), new Fields(info.get_output_fields()));
        }
    }
}
