package backtype.storm.utils;

import backtype.storm.Constants;
import clojure.lang.PersistentArrayMap;
import java.util.Iterator;
import java.util.HashMap;
import java.util.Map;
import org.json.simple.JSONObject;
import org.json.simple.JSONValue;


public class LocalStateSerializer implements StateSerializer {
    public byte[] serializeState (Map<Object, Object> val) {
        /*
          for (Map.Entry<Object,Object> entry : val.entrySet()) {
          Object v = entry.getValue();

          if (v instanceof String) {
          JSONObject ser = new JSONObject();
          }
          else if(v instanceof clojure.lang.PersistentArrayMap) {
          if (((Map) v).size() > 0) {
          System.err.println(((Map) v).size());
          System.err.println("cow");
          System.err.println(JSONValue.toJSONString(val));
          System.err.println(JSONValue.parse(JSONValue.toJSONString(val)));
          throw new RuntimeException(((Map) v).size() + " " + entry.getValue().getClass().toString());
          }
          }
          else
          throw new RuntimeException(v.getClass().toString());
          }
        */
        // LS_ID : String
        // LS_WORKER_HEARTBEAT : backtype.storm.daemon.common.WorkerHeartbeat
        // LS_LOCAL_ASSIGNMENTS : clojure.lang.PersistentArrayMap
        // LS_APPROVED_WORKERS : clojure.lang.PersistentArrayMap
        HashMap<Object,Object> toSerialize = new HashMap<Object,Object>();
        for (Map.Entry<Object, Object> entry : val.entrySet()) {
            String k = (String) entry.getKey();
            Object v = entry.getValue();
            
            if (k.equals(Constants.LS_WORKER_HEARTBEAT))
                // ser worker heartbeat
                ;
            else if (k.equals(Constants.LS_APPROVED_WORKERS))
                toSerialize.put(k, serLsApprovedWorkers((PersistentArrayMap) v));
            else if (k.equals(Constants.LS_ID))
                toSerialize.put(k, v);
            else if (k.equals(Constants.LS_LOCAL_ASSIGNMENTS))
                toSerialize.put(k, serLsLocalAssignments((PersistentArrayMap) v));
            else
                throw new RuntimeException("LocalState could not be " +
                                           "serialized; procedure for key " +
                                           k + " has not been implemented");
        }
        //throw new RuntimeException("not implemented");
        return Utils.serialize(val);
    }
    
    public String serLsLocalAssignments (PersistentArrayMap assg) {
        return JSONValue.toJSONString(assg);
    }
    
    public String serLsApprovedWorkers (PersistentArrayMap workers) {
        /*
        Iterator<clojure.lang.IMapEntry> iter = workers.iterator();
        while (iter.hasNext()) {
            clojure.lang.IMapEntry kv = iter.next();
            System.err.println(kv);
            System.err.println(kv.key());
            System.err.println(kv.val());
        }
        */
        return JSONValue.toJSONString(workers);
    }
    
    public Map<Object, Object> deserializeState (byte[] ser) {
        /*
          String s = new String(serialized);
          //System.err.println("DESER " + JSONValue.parse(s));
          Object parsed = JSONValue.parse(s);
          if (parsed != null)
          return parsed;
          else
          return new HashMap<Object, Object>();
        */
        return (Map<Object, Object>) Utils.deserialize(ser);
    }
    /*
      private static byte[] serializeLocalState(Map<Object, Object> val) {
      //System.err.println(JSONValue.toJSONString(val));
      //System.err.println("SER " + JSONValue.toJSONString(val));
      //return JSONValue.toJSONString(val).getBytes();
      }

      private static Object deserializeLocalState(byte[] serialized) {
      }
    */
}