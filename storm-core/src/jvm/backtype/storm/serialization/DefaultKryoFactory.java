package backtype.storm.serialization;

import backtype.storm.Config;
import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import java.util.Map;


public class DefaultKryoFactory implements IKryoFactory {

    public static class KryoSerializableDefault extends Kryo {
        boolean _override = false;
        
        public void overrideDefault(boolean value) {
            _override = value;
        }                
        
        @Override
        public Serializer getDefaultSerializer(Class type) {
            if(_override) {
                return new SerializableSerializer();
            } else {
                return super.getDefaultSerializer(type);
            }
        }        
    }    
    
    @Override
    public Kryo getKryo(Map conf) {
        KryoSerializableDefault k = new KryoSerializableDefault();
        k.setRegistrationRequired(!((Boolean) conf.get(Config.TOPOLOGY_FALL_BACK_ON_JAVA_SERIALIZATION)));        
        k.setReferences(false);
        return k;
    }

    @Override
    public void preRegister(Kryo k, Map conf) {
    }
    
    public void postRegister(Kryo k, Map conf) {
        ((KryoSerializableDefault)k).overrideDefault(true);
    }

    @Override
    public void postDecorate(Kryo k, Map conf) {        
    }    
}
