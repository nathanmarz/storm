package backtype.storm.serialization;

import backtype.storm.utils.WritableUtils;
import java.io.DataInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;


public class ValuesDeserializer {
    SerializationFactory _factory;
    List<FieldSerialization> _deserializers;
    
    public ValuesDeserializer(Map conf) {
        _factory = new SerializationFactory(conf);        
    }
    
    public List<Object> deserializeFrom(DataInputStream in) throws IOException {
        int numValues = WritableUtils.readVInt(in);
        if(_deserializers==null) {
            _deserializers = new ArrayList<FieldSerialization>();
            for(int i=0; i<numValues; i++) {
                _deserializers.add(null);
            }                    
        }
        if(numValues!=_deserializers.size()) {
            throw new RuntimeException("Received a tuple with an unexpected number of fields");
        }
        if(numValues==0) return null;
        List<Object> values = new ArrayList<Object>(numValues);
        for(int i=0; i<_deserializers.size(); i++) {
            int token = WritableUtils.readVInt(in);
            if(token==0) {
                values.add(null);
            } else {
                if(_deserializers.get(i)==null) {
                    _deserializers.set(i, _factory.getSerializationForToken(token));
                }
                FieldSerialization fser = _deserializers.get(i);
                if(token!=fser.getToken()) {
                    throw new RuntimeException("Received field of different types " + token + " " + fser.getToken());
                }
                values.add(fser.deserialize(in));
            }
        }        
        return values;        
    }
}
