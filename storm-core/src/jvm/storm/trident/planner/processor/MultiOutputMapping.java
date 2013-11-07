package storm.trident.planner.processor;

import java.io.Serializable;
import java.util.Map;
import java.util.Set;
import java.util.HashMap;
import backtype.storm.tuple.Fields;
import storm.trident.planner.TupleReceiver;

/**
   Maintains, naively, a mapping from stream names to fields and tuple
   receivers. Each stream should only ever (in the current impl of a
   multi-each) have one receiver.
   <p>
   FIXME: Does much of the work that the TridentContext
   is supposed to do.
 */
public class MultiOutputMapping implements Serializable {

    protected Map<String,TupleReceiver> _map;
    protected Map<String,Fields> _fields;
    
    public MultiOutputMapping() {
        _map = new HashMap<String,TupleReceiver>();
        _fields = new HashMap<String,Fields>();
    }

    public void addStream(String name, Fields fields, TupleReceiver rcvr) {
        _map.put(name, rcvr);
        _fields.put(name, fields);
    }

    public TupleReceiver getReceiver(String name) {
        return _map.get(name);
    }

    public Set<String> getStreamNames() {
        return _map.keySet();
    }

    public Fields getFields(String name) {
        return _fields.get(name);
    }
}
