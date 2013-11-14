package storm.trident.tuple;

import backtype.storm.tuple.Fields;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.lang.builder.ToStringBuilder;

public class ValuePointer {
    public static Map<String, ValuePointer> buildFieldIndex(ValuePointer[] pointers) {
        Map<String, ValuePointer> ret = new HashMap<String, ValuePointer>();
        for(ValuePointer ptr: pointers) {
            ret.put(ptr.field, ptr);
        }
        return ret;        
    }

    public static ValuePointer[] buildIndex(Fields fieldsOrder, Map<String, ValuePointer> pointers) {
        if(fieldsOrder.size()!=pointers.size()) {
            throw new IllegalArgumentException("Fields order must be same length as pointers map");
        }
        ValuePointer[] ret = new ValuePointer[pointers.size()];
        List<String> flist = fieldsOrder.toList();
        for(int i=0; i<fieldsOrder.size(); i++) {
            ret[i] = pointers.get(fieldsOrder.get(i));
        }
        return ret;
    }    
    
    public int delegateIndex;
    protected int index;
    protected String field;
    
    public ValuePointer(int delegateIndex, int index, String field) {
        this.delegateIndex = delegateIndex;
        this.index = index;
        this.field = field;
    }

    @Override
    public String toString() {
        return ToStringBuilder.reflectionToString(this);
    }    
}
