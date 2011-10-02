package backtype.storm.spout;

import backtype.storm.generated.ShellComponent;
import backtype.storm.task.TopologyContext;
import java.util.Map;


public class ShellSpout implements ISpout {
    public ShellSpout(ShellComponent component) {
        this(component.get_execution_command(), component.get_script());
    }
    
    public ShellSpout(String shellCommand, String codeResource) {
        
    }
    
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    public void close() {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    public void nextTuple() {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    public void ack(Object msgId) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    public void fail(Object msgId) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

}
