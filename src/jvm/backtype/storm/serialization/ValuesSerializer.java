package backtype.storm.serialization;

import backtype.storm.utils.WritableUtils;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;


/**
 * Expects to see tuples of same size and field types
 */
public class ValuesSerializer {
    List<FieldSerialization> _serializers;
    SerializationFactory _factory;

    public ValuesSerializer(Map conf) {
        _factory = new SerializationFactory(conf);
    }
    
    public void serializeInto(List values, DataOutputStream out) throws IOException {
        int numValues = 0;
        if(values!=null) {
            numValues = values.size();
        }
        if(_serializers==null) {
            _serializers = new ArrayList<FieldSerialization>();
            for(int i=0; i<numValues; i++) {
                _serializers.add(null);
            }
        }


        if(_serializers.size()!=numValues) {
            throw new RuntimeException("Received unexpected tuple with different size than a prior tuple on this stream " + values.toString());
        }
        
        WritableUtils.writeVInt(out, numValues);
        for(int i=0; i<_serializers.size(); i++) {
            Object val = values.get(i);
            if(val==null) {
                WritableUtils.writeVInt(out, 0);
            } else {
                if(_serializers.get(i)==null) {
                    _serializers.set(i, _factory.getSerializationForClass(val.getClass()));
                }
                FieldSerialization fs = _serializers.get(i);
                WritableUtils.writeVInt(out, fs.getToken());
                fs.serialize(values.get(i), out);
            }
        }
    }
}
