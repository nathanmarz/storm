package storm.trident.fluent;

import java.util.Map;
import java.util.List;
import java.util.ArrayList;
import java.util.Set;
import java.io.Serializable;
import backtype.storm.tuple.Fields;

public class StreamMap implements Serializable {
    protected Map<String, Fields> _map;   // Logical map from stream names to fields
    protected Fields _allOutputFields;
    
    
    public StreamMap(Map<String,Fields> map) {
        _map = map;
    }

    public Set<String> getStreamNames() {
        return _map.keySet();
    }

    public Fields getFields(String streamName) {
        Fields f = _map.get(streamName);        
        //return new Fields(namespaceFields(streamName,f));
        return f;
    }
    
    /**
       Returns the output fields of all the declared
       streams, scoped by their respective streams
     */
    public Fields getScopedOutputFields() {
        if (_allOutputFields != null)
            return _allOutputFields;
        
        List<String> resultFields = new ArrayList<String>();
        for (Map.Entry<String, Fields> streamDeclaration : _map.entrySet()) {
            String streamName = streamDeclaration.getKey();
            Fields outputFields = streamDeclaration.getValue();
            resultFields.addAll(namespaceFields(streamName, outputFields));
        }
        _allOutputFields = new Fields(resultFields);
        return _allOutputFields;
    }

    private List<String> namespaceFields(String nameSpace, Fields fields) {
        List<String> namespaced = new ArrayList<String>(fields.size());
        for (String field : fields) {
            namespaced.add(nameSpace + "::" + field);
        }
        return namespaced;
    }
}
