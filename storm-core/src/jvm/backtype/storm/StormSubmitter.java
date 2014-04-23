/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package backtype.storm;

import backtype.storm.generated.*;
import backtype.storm.utils.BufferFileInputStream;
import backtype.storm.utils.NimbusClient;
import backtype.storm.utils.Utils;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.thrift.TException;
import org.json.simple.JSONValue;

/**
 * Use this class to submit topologies to run on the Storm cluster. You should run your program
 * with the "storm jar" command from the command-line, and then use this class to
 * submit your topologies.
 */
public class StormSubmitter {
    public static Logger LOG = LoggerFactory.getLogger(StormSubmitter.class);    

    private static Nimbus.Iface localNimbus = null;

    public static void setLocalNimbus(Nimbus.Iface localNimbusHandler) {
        StormSubmitter.localNimbus = localNimbusHandler;
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
        submitTopology(name, stormConf, topology, null);
    }    
    
    /**
     * Submits a topology to run on the cluster. A topology runs forever or until 
     * explicitly killed.
     *
     *
     * @param name the name of the storm.
     * @param stormConf the topology-specific configuration. See {@link Config}. 
     * @param topology the processing to execute.
     * @param options to manipulate the starting of the topology
     * @throws AlreadyAliveException if a topology with this name is already running
     * @throws InvalidTopologyException if an invalid topology was submitted
     */
    public static void submitTopology(String name, Map stormConf, StormTopology topology, SubmitOptions opts) throws AlreadyAliveException, InvalidTopologyException {
        if(!Utils.isValidConf(stormConf)) {
            throw new IllegalArgumentException("Storm conf is not valid. Must be json-serializable");
        }
        stormConf = new HashMap(stormConf);
        stormConf.putAll(Utils.readCommandLineOpts());
        Map conf = Utils.readStormConfig();
        conf.putAll(stormConf);
        try {
            String serConf = JSONValue.toJSONString(stormConf);
            if(localNimbus!=null) {
                LOG.info("Submitting topology " + name + " in local mode");
                localNimbus.submitTopology(name, null, serConf, topology);
            } else {
                NimbusClient client = NimbusClient.getConfiguredClient(conf);
                if(topologyNameExists(conf, name)) {
                    throw new RuntimeException("Topology with name `" + name + "` already exists on cluster");
                }
                submitJar(conf);
                try {
                    LOG.info("Submitting topology " +  name + " in distributed mode with conf " + serConf);
                    if(opts!=null) {
                        client.getClient().submitTopologyWithOpts(name, submittedJar, serConf, topology, opts);                    
                    } else {
                        // this is for backwards compatibility
                        client.getClient().submitTopology(name, submittedJar, serConf, topology);                                            
                    }
                } catch(InvalidTopologyException e) {
                    LOG.warn("Topology submission exception: "+e.get_msg());
                    throw e;
                } catch(AlreadyAliveException e) {
                    LOG.warn("Topology already alive exception", e);
                    throw e;
                } finally {
                    client.close();
                }
            }
            LOG.info("Finished submitting topology: " +  name);
        } catch(TException e) {
            throw new RuntimeException(e);
        }
    }
    
    private static boolean topologyNameExists(Map conf, String name) {
        NimbusClient client = NimbusClient.getConfiguredClient(conf);
        try {
            ClusterSummary summary = client.getClient().getClusterInfo();
            for(TopologySummary s : summary.get_topologies()) {
                if(s.get_name().equals(name)) {  
                    return true;
                } 
            }  
            return false;

        } catch(Exception e) {
            throw new RuntimeException(e);
        } finally {
            client.close();
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
        if(localJar==null) {
            throw new RuntimeException("Must submit topologies using the 'storm' client script so that StormSubmitter knows which jar to upload.");
        }
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
