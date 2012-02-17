package backtype.storm;

import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.generated.Nimbus;
import backtype.storm.generated.StormTopology;
import backtype.storm.utils.BufferFileInputStream;
import backtype.storm.utils.NimbusClient;
import backtype.storm.utils.Utils;
import java.nio.ByteBuffer;
import java.util.Map;
import org.apache.log4j.Logger;
import org.apache.thrift7.TException;
import org.json.simple.JSONValue;

/**
 * Use this class to submit topologies to run on the Storm cluster. You should run your program
 * with the "storm jar" command from the command-line, and then use this class to
 * submit your topologies.
 */
public class StormSubmitter {
    public static Logger LOG = Logger.getLogger(StormSubmitter.class);    

    private static Nimbus.Iface localNimbus = null;

    public static void setLocalNimbus(Nimbus.Iface localNimbusHandler) {
        StormSubmitter.localNimbus = localNimbusHandler;
    }

    /**
     * compares whether the two maps equals.
     * 
     * MARK: we need this method since JSONValue has this bug:
     *      have a look at the following code:
     *      <code>
     *          Map map = new HashMap();
     *          map.put("foo", 1);
     *          
     *          String json = JSONValue.toJSONString(map);
     *          JSONObject target = JSONValue.parse(json);
     *          boolean result = map.equals(target); 
     *      </code>
     *      
     *      the result will be false, because before convert
     *      the map to json, 1 is an integer; after you convert
     *      the json back to a JSONObject(Map), 1 becomes Long
     *      an Integer will never equals to a Long, so we need to 
     *      customize this compareMap method.
     * @param map1
     * @param map2
     * @return
     */
    private static boolean compareMap(Map map1, Map map2) {
        if (map1 == null && map2 == null) {
            return true;
        } else if ((map1 == null && map2 != null)
                || (map1 != null && map2 == null)) {
            return false;
        } else {
            if (map1.size() != map2.size()) {
                return false;
            } else {
                for (Object keyObj : map1.keySet()) {
                    String key = (String) keyObj;

                    Object value1 = map1.get(key);
                    Object value2 = map2.get(key);

                    // for Integer && Long, we need to write custom logic
                    if ((value1 instanceof Integer && value2 instanceof Long)
                            || (value1 instanceof Long && value2 instanceof Integer)) {
                        long longValue1 = Long.parseLong(value1.toString());
                        long longValue2 = Long.parseLong(value2.toString());

                        if (longValue1 == longValue2) {
                            continue;
                        } else {
                            return false;
                        }
                    } else {
                        // for other types
                        if ((value1 == null && value2 != null)
                                || (value1 != null && value2 == null)) {
                            return false;
                        } else if (value1 == null && value2 == null) {
                            continue;
                        } else {
                            if (value1.equals(value2)) {
                                continue;
                            }
                        }
                    }
                }

            }
        }

        return true;
    }

    /**
     * validate whether there are invalid config in the stormConf.
     */    
    public static void validateStormConf(Map stormConf) {
        if (stormConf == null) {
            return;
        }

        String json = JSONValue.toJSONString(stormConf);
        Map target = (Map) JSONValue.parse(json);

        // after convert map to json, and then convert json back to map
        // if the object does not equals to stormConf, there must be some
        // invalid config
        if (!compareMap(stormConf, target)) {
            throw new IllegalArgumentException(
                    "invaild config passed in, mostly because you put a java bean in the config"
                     + ", only json types(number, string, boolean, null, list, map etc) are supported");
        }
    }

    /**
     * Submits a topology to run on the cluster. A topology runs forever or until 
     * explicitly killed.
     *
     *
     * @param name the name of the storm.
     * @param stormConf the topology-specific configuration. See {@link Config}. 
     * @param topology the processing to execute.
     * @throws AlreadyAliveException if a topology with this name is already running
     * @throws InvalidTopologyException if an invalid topology was submitted
     */
    public static void submitTopology(String name, Map stormConf, StormTopology topology) throws AlreadyAliveException, InvalidTopologyException {
        // validate the config first
        validateStormConf(stormConf);

        Map conf = Utils.readStormConfig();
        conf.putAll(stormConf);
        try {
            String serConf = JSONValue.toJSONString(stormConf);
            if(localNimbus!=null) {
                LOG.info("Submitting topology " + name + " in local mode");
                localNimbus.submitTopology(name, null, serConf, topology);
            } else {
                submitJar(conf);
                NimbusClient client = NimbusClient.getConfiguredClient(conf);
                try {
                    LOG.info("Submitting topology " +  name + " in distributed mode with conf " + serConf);
                    client.getClient().submitTopology(name, submittedJar, serConf, topology);
                } finally {
                    client.close();
                }
            }
            LOG.info("Finished submitting topology: " +  name);
        } catch(TException e) {
            throw new RuntimeException(e);
        }
    }

    private static String submittedJar = null;
    
    private static void submitJar(Map conf) {
        if(submittedJar==null) {
            LOG.info("Jar not uploaded to master yet. Submitting jar...");
            String localJar = System.getProperty("storm.jar");
            submittedJar = submitJar(conf, localJar);
        } else {
            LOG.info("Jar already uploaded to master. Not submitting jar.");
        }
    }
    
    public static String submitJar(Map conf, String localJar) {
        NimbusClient client = NimbusClient.getConfiguredClient(conf);
        try {
            String uploadLocation = client.getClient().beginFileUpload();
            LOG.info("Uploading topology jar " + localJar + " to assigned location: " + uploadLocation);
            BufferFileInputStream is = new BufferFileInputStream(localJar);
            while(true) {
                byte[] toSubmit = is.read();
                if(toSubmit.length==0) break;
                client.getClient().uploadChunk(uploadLocation, ByteBuffer.wrap(toSubmit));
            }
            client.getClient().finishFileUpload(uploadLocation);
            LOG.info("Successfully uploaded topology jar to assigned location: " + uploadLocation);
            return uploadLocation;
        } catch(Exception e) {
            throw new RuntimeException(e);            
        } finally {
            client.close();
        }
    }
}
