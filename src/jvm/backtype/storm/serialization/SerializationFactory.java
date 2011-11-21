package backtype.storm.serialization;

import backtype.storm.Config;
import backtype.storm.generated.ComponentCommon;
import backtype.storm.generated.StormTopology;
import backtype.storm.utils.ListDelegate;
import backtype.storm.utils.Utils;
import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.ObjectBuffer;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.serialize.SerializableSerializer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import org.apache.log4j.Logger;

public class SerializationFactory {
    public static final Logger LOG = Logger.getLogger(SerializationFactory.class);
    
    public static class KryoSerializableDefault extends Kryo {
        boolean _override = false;
        
        public void overrideDefault(boolean value) {
            _override = value;
        }
        
        @Override
        protected Serializer newDefaultSerializer(Class type) {
            if(_override) {
                return new SerializableSerializer();
            } else {
                return super.newDefaultSerializer(type);
            }
        }        
    }
    
    public static ObjectBuffer getKryo(Map conf) {
        KryoSerializableDefault k = new KryoSerializableDefault();
        k.setRegistrationOptional((Boolean) conf.get(Config.TOPOLOGY_FALL_BACK_ON_JAVA_SERIALIZATION));
        k.register(ListDelegate.class);
        k.register(ArrayList.class);
        k.register(HashMap.class);
        k.register(HashSet.class);
        // TODO: do clojure persistent data structures
        Map<String, String> registrations = (Map<String, String>) conf.get(Config.TOPOLOGY_KRYO_REGISTER);
        if(registrations==null) registrations = new HashMap<String, String>();
        boolean skipMissing = (Boolean) conf.get(Config.TOPOLOGY_SKIP_MISSING_KRYO_REGISTRATIONS);
        for(String klassName: registrations.keySet()) {
            String serializerClassName = registrations.get(klassName);
            try {
                Class klass = Class.forName(klassName);
                Class serializerClass = null;
                if(serializerClassName!=null)
                    serializerClass = Class.forName(serializerClassName);
                if(serializerClass == null) {
                    k.register(klass);
                } else {
                    k.register(klass, (Serializer) serializerClass.newInstance());
                }
                
            } catch (ClassNotFoundException e) {
                if(skipMissing) {
                    LOG.info("Could not find serialization or class for " + serializerClassName + ". Skipping registration...");
                } else {
                    throw new RuntimeException(e);
                }
            } catch (InstantiationException e) {
                throw new RuntimeException(e);
            } catch (IllegalAccessException e) {
                throw new RuntimeException(e);                
            }
        }
        k.overrideDefault(true);
        return new ObjectBuffer(k, 2000, 2000000000);        
    }
    
    public static class IdDictionary {        
        Map<String, Map<String, Integer>> streamNametoId = new HashMap<String, Map<String, Integer>>();
        Map<String, Map<Integer, String>> streamIdToName = new HashMap<String, Map<Integer, String>>();

        public IdDictionary(StormTopology topology) {
            // TODO: need to handle system streams and system components
            // should bite the bullet and expose user topology and system topology
            // should create system topology in nimbus and use that for everything. user topology
            // is just for querying
            List<String> componentNames = new ArrayList<String>(topology.get_spouts().keySet());
            componentNames.addAll(topology.get_bolts().keySet());
            componentNames.addAll(topology.get_state_spouts().keySet());
                        
            for(String name: componentNames) {
                ComponentCommon common = Utils.getComponentCommon(topology, name);
                List<String> streams = new ArrayList<String>(common.get_streams().keySet());
                streamNametoId.put(name, idify(streams));
                streamIdToName.put(name, Utils.reverseMap(streamNametoId.get(name)));
            }
        }
                
        public int getStreamId(String component, String stream) {
            return streamNametoId.get(component).get(stream);
        }
        
        public String getStreamName(String component, int stream) {
            return streamIdToName.get(component).get(stream);
        }

        private static Map<String, Integer> idify(List<String> names) {
            Collections.sort(names);
            Map<String, Integer> ret = new HashMap<String, Integer>();
            int i = 1;
            for(String name: names) {
                ret.put(name, i);
                i++;
            }
            return ret;
        }
    }    
}
