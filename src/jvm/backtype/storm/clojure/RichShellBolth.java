package backtype.storm.clojure;

import backtype.storm.generated.StreamInfo;
import backtype.storm.task.Shellbolth;
import backtype.storm.topology.IRichbolth;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import java.util.Map;

public class RichShellbolth extends Shellbolth implements IRichbolth {
    private Map<String, StreamInfo> _outputs;
    
    public RichShellbolth(String[] command, Map<String, StreamInfo> outputs) {
        super(command);
        _outputs = outputs;
    }
    
    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        for(String stream: _outputs.keySet()) {
            StreamInfo def = _outputs.get(stream);
            if(def.is_direct()) {
                declarer.declareStream(stream, true, new Fields(def.get_output_fields()));
            } else {
                declarer.declareStream(stream, new Fields(def.get_output_fields()));                
            }
        }
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }    
}
