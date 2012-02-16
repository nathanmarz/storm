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
import java.util.Set;
import java.util.HashSet;
import java.util.Arrays;
import java.util.List;
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

    /** these topology config should be type of boolean */
    private static final Set<String> BOOLEAN_CONFIGS = new HashSet<String>(
            Arrays.asList(new String[]{
                Config.TOPOLOGY_DEBUG,
                Config.TOPOLOGY_OPTIMIZE,
                Config.TOPOLOGY_SKIP_MISSING_KRYO_REGISTRATIONS,
                Config.TOPOLOGY_FALL_BACK_ON_JAVA_SERIALIZATION
            })
        );
        
    /** these topology config should be type of integer */
    private static final Set<String> INT_CONFIGS = new HashSet<String>(
                Arrays.asList(new String[]{
                    Config.TOPOLOGY_WORKERS,
                    Config.TOPOLOGY_ACKERS,
                    Config.TOPOLOGY_MESSAGE_TIMEOUT_SECS,
                    Config.TOPOLOGY_MAX_TASK_PARALLELISM,
                    Config.TOPOLOGY_MAX_SPOUT_PENDING,
                    Config.TOPOLOGY_STATE_SYNCHRONIZATION_TIMEOUT_SECS,
                    
                })
            );
        
    /** these topology config should be type of list */
    private static final Set<String> LIST_CONFIGS = new HashSet<String>(
                Arrays.asList(new String[]{
                    Config.TOPOLOGY_KRYO_REGISTER,
                })
            );
        
    /** these topology config should be type of float */
    private static final Set<String> DOUBLE_CONFIGS = new HashSet<String>(
                Arrays.asList(new String[]{
                    Config.TOPOLOGY_STATS_SAMPLE_RATE
                })
            );
        
    /** these topology config should be type of String */
    private static final Set<String> STRING_CONFIGS = new HashSet<String>(
                Arrays.asList(new String[]{
                    Config.TOPOLOGY_WORKER_CHILDOPTS,
                    Config.TOPOLOGY_TRANSACTIONAL_ID
                })
            );

    private static Nimbus.Iface localNimbus = null;

    public static void setLocalNimbus(Nimbus.Iface localNimbusHandler) {
        StormSubmitter.localNimbus = localNimbusHandler;
    }


    private static void validateStromConfType(String key, Object value, Class expectedType) {
        if (!expectedType.isInstance(value)) {
            throw new IllegalArgumentException("storm config: [" + key + "] should be type of " + expectedType.getCanonicalName() + ".");
        }
    }

    /**
     * validate the stormConf
     */    
    public static void validateStormConf(Map stormConf) {
        if (stormConf == null) {
            return;
        }
        for (Object keyObj : stormConf.keySet()) {
            if (!(keyObj instanceof String)) {
                throw new IllegalArgumentException("storm config key should be type of string.");
            }
            String key = (String)keyObj;
            Object value = stormConf.get(key);
            
            if (BOOLEAN_CONFIGS.contains(key)) {
                validateStromConfType(key, value, Boolean.class);
            } else if (INT_CONFIGS.contains(key)) {
                validateStromConfType(key, value, Integer.class);
            } else if (LIST_CONFIGS.contains(key)) {
                validateStromConfType(key, value, List.class);
            } else if (DOUBLE_CONFIGS.contains(key)) {
                validateStromConfType(key, value, Double.class);
            } else if (STRING_CONFIGS.contains(key)) {
                validateStromConfType(key, value, String.class);
            } else {
                throw new IllegalArgumentException("your config: " + key 
                                                   + " can not be recognized or is not a topology-specific config");
            }
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
