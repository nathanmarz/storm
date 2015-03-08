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

import java.io.File;
import java.nio.ByteBuffer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.apache.thrift.TException;
import org.json.simple.JSONValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.security.auth.IAutoCredentials;
import backtype.storm.security.auth.AuthUtils;
import backtype.storm.generated.*;
import backtype.storm.utils.BufferFileInputStream;
import backtype.storm.utils.NimbusClient;
import backtype.storm.utils.Utils;

/**
 * Use this class to submit topologies to run on the Storm cluster. You should run your program
 * with the "storm jar" command from the command-line, and then use this class to
 * submit your topologies.
 */
public class StormSubmitter {
    public static Logger LOG = LoggerFactory.getLogger(StormSubmitter.class);    

    private static final int THRIFT_CHUNK_SIZE_BYTES = 307200;
    
    private static ILocalCluster localNimbus = null;

    public static void setLocalNimbus(ILocalCluster localNimbusHandler) {
        StormSubmitter.localNimbus = localNimbusHandler;
    }

    private static String generateZookeeperDigestSecretPayload() {
        return Utils.secureRandomLong() + ":" + Utils.secureRandomLong();
    }

    public static final Pattern zkDigestPattern = Pattern.compile("\\S+:\\S+");

    public static boolean validateZKDigestPayload(String payload) {
        if (payload != null) {
            Matcher m = zkDigestPattern.matcher(payload);
            return m.matches();
        }
        return false;
    }

    @SuppressWarnings("unchecked")
    public static Map prepareZookeeperAuthentication(Map conf) {
        Map toRet = new HashMap();

        // Is the topology ZooKeeper authentication configuration unset?
        if (! conf.containsKey(Config.STORM_ZOOKEEPER_TOPOLOGY_AUTH_PAYLOAD) ||
                conf.get(Config.STORM_ZOOKEEPER_TOPOLOGY_AUTH_PAYLOAD) == null || 
                !  validateZKDigestPayload((String)
                    conf.get(Config.STORM_ZOOKEEPER_TOPOLOGY_AUTH_PAYLOAD))) {

            String secretPayload = generateZookeeperDigestSecretPayload();
            toRet.put(Config.STORM_ZOOKEEPER_TOPOLOGY_AUTH_PAYLOAD, secretPayload);
            LOG.info("Generated ZooKeeper secret payload for MD5-digest: " + secretPayload);
        }
        
        // This should always be set to digest.
        toRet.put(Config.STORM_ZOOKEEPER_TOPOLOGY_AUTH_SCHEME, "digest");

        return toRet;
    }

    private static Map<String,String> populateCredentials(Map conf, Map<String, String> creds) {
        Map<String,String> ret = new HashMap<String,String>();
        for (IAutoCredentials autoCred: AuthUtils.GetAutoCredentials(conf)) {
            LOG.info("Running "+autoCred);
            autoCred.populateCredentials(ret);
        }
        if (creds != null) {
            ret.putAll(creds);
        }
        return ret;
    }

    /**
     * Push a new set of credentials to the running topology.
     * @param name the name of the topology to push credentials to.
     * @param stormConf the topology-specific configuration, if desired. See {@link Config}. 
     * @param credentials the credentials to push.
     * @throws AuthorizationException if you are not authorized ot push credentials.
     * @throws NotAliveException if the topology is not alive
     * @throws InvalidTopologyException if any other error happens
     */
    public static void pushCredentials(String name, Map stormConf, Map<String, String> credentials) 
            throws AuthorizationException, NotAliveException, InvalidTopologyException {
        stormConf = new HashMap(stormConf);
        stormConf.putAll(Utils.readCommandLineOpts());
        Map conf = Utils.readStormConfig();
        conf.putAll(stormConf);
        Map<String,String> fullCreds = populateCredentials(conf, credentials);
        if (fullCreds.isEmpty()) {
            LOG.warn("No credentials were found to push to "+name);
            return;
        }
        try {
            if(localNimbus!=null) {
                LOG.info("Pushing Credentials to topology " + name + " in local mode");
                localNimbus.uploadNewCredentials(name, new Credentials(fullCreds));
            } else {
                NimbusClient client = NimbusClient.getConfiguredClient(conf);
                try {
                    LOG.info("Uploading new credentials to " +  name);
                    client.getClient().uploadNewCredentials(name, new Credentials(fullCreds));
                } finally {
                    client.close();
                }
            }
            LOG.info("Finished submitting topology: " +  name);
        } catch(TException e) {
            throw new RuntimeException(e);
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
     * @throws AuthorizationException if authorization is failed
     */
    public static void submitTopology(String name, Map stormConf, StormTopology topology) 
            throws AlreadyAliveException, InvalidTopologyException, AuthorizationException {
        submitTopology(name, stormConf, topology, null, null);
    }    

    /**
     * Submits a topology to run on the cluster. A topology runs forever or until 
     * explicitly killed.
     *
     * @param name the name of the storm.
     * @param stormConf the topology-specific configuration. See {@link Config}. 
     * @param topology the processing to execute.
     * @param opts to manipulate the starting of the topology.
     * @throws AlreadyAliveException if a topology with this name is already running
     * @throws InvalidTopologyException if an invalid topology was submitted
     * @throws AuthorizationException if authorization is failed
     */
    public static void submitTopology(String name, Map stormConf, StormTopology topology, SubmitOptions opts) 
            throws AlreadyAliveException, InvalidTopologyException, AuthorizationException {
        submitTopology(name, stormConf, topology, opts, null);
    }

    /**
     *
     * @param name
     * @param stormConf
     * @param topology
     * @param opts
     * @param progressListener
     * @param asUser The user as which this topology should be submitted.
     * @throws AlreadyAliveException
     * @throws InvalidTopologyException
     * @throws AuthorizationException
     */
    public static void submitTopologyAs(String name, Map stormConf, StormTopology topology, SubmitOptions opts, ProgressListener progressListener, String asUser)
            throws AlreadyAliveException, InvalidTopologyException, AuthorizationException {
        if(!Utils.isValidConf(stormConf)) {
            throw new IllegalArgumentException("Storm conf is not valid. Must be json-serializable");
        }
        stormConf = new HashMap(stormConf);
        stormConf.putAll(Utils.readCommandLineOpts());
        Map conf = Utils.readStormConfig();
        conf.putAll(stormConf);
        stormConf.putAll(prepareZookeeperAuthentication(conf));

        Map<String,String> passedCreds = new HashMap<String, String>();
        if (opts != null) {
            Credentials tmpCreds = opts.get_creds();
            if (tmpCreds != null) {
                passedCreds = tmpCreds.get_creds();
            }
        }
        Map<String,String> fullCreds = populateCredentials(conf, passedCreds);
        if (!fullCreds.isEmpty()) {
            if (opts == null) {
                opts = new SubmitOptions(TopologyInitialStatus.ACTIVE);
            }
            opts.set_creds(new Credentials(fullCreds));
        }
        try {
            if(localNimbus!=null) {
                LOG.info("Submitting topology " + name + " in local mode");
                if(opts!=null) {
                    localNimbus.submitTopologyWithOpts(name, stormConf, topology, opts);
                } else {
                    // this is for backwards compatibility
                    localNimbus.submitTopology(name, stormConf, topology);
                }
            } else {
                String serConf = JSONValue.toJSONString(stormConf);
                NimbusClient client = NimbusClient.getConfiguredClientAs(conf, asUser);
                if(topologyNameExists(conf, name, asUser)) {
                    throw new RuntimeException("Topology with name `" + name + "` already exists on cluster");
                }
                String jar = submitJarAs(conf, System.getProperty("storm.jar"), progressListener, asUser);
                try {
                    LOG.info("Submitting topology " +  name + " in distributed mode with conf " + serConf);
                    if(opts!=null) {
                        client.getClient().submitTopologyWithOpts(name, jar, serConf, topology, opts);
                    } else {
                        // this is for backwards compatibility
                        client.getClient().submitTopology(name, jar, serConf, topology);
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

    /**
     * Submits a topology to run on the cluster. A topology runs forever or until
     * explicitly killed.
     *
     *
     * @param name the name of the storm.
     * @param stormConf the topology-specific configuration. See {@link Config}.
     * @param topology the processing to execute.
     * @param opts to manipulate the starting of the topology
     * @param progressListener to track the progress of the jar upload process
     * @throws AlreadyAliveException if a topology with this name is already running
     * @throws InvalidTopologyException if an invalid topology was submitted
     * @throws AuthorizationException if authorization is failed
     */
    @SuppressWarnings("unchecked")
    public static void submitTopology(String name, Map stormConf, StormTopology topology, SubmitOptions opts,
             ProgressListener progressListener) throws AlreadyAliveException, InvalidTopologyException, AuthorizationException {
        submitTopologyAs(name, stormConf, topology, opts, progressListener, null);
    }

    /**
     * Submits a topology to run on the cluster with a progress bar. A topology runs forever or until
     * explicitly killed.
     *
     *
     * @param name the name of the storm.
     * @param stormConf the topology-specific configuration. See {@link Config}.
     * @param topology the processing to execute.
     * @throws AlreadyAliveException if a topology with this name is already running
     * @throws InvalidTopologyException if an invalid topology was submitted
     * @throws AuthorizationException if authorization is failed
     */

    public static void submitTopologyWithProgressBar(String name, Map stormConf, StormTopology topology) throws AlreadyAliveException, InvalidTopologyException, AuthorizationException {
        submitTopologyWithProgressBar(name, stormConf, topology, null);
    }

    /**
     * Submits a topology to run on the cluster with a progress bar. A topology runs forever or until
     * explicitly killed.
     *
     *
     * @param name the name of the storm.
     * @param stormConf the topology-specific configuration. See {@link Config}.
     * @param topology the processing to execute.
     * @param opts to manipulate the starting of the topology
     * @throws AlreadyAliveException if a topology with this name is already running
     * @throws InvalidTopologyException if an invalid topology was submitted
     * @throws AuthorizationException if authorization is failed
     */

    public static void submitTopologyWithProgressBar(String name, Map stormConf, StormTopology topology, SubmitOptions opts) throws AlreadyAliveException, InvalidTopologyException, AuthorizationException {
        // show a progress bar so we know we're not stuck (especially on slow connections)
        submitTopology(name, stormConf, topology, opts, new StormSubmitter.ProgressListener() {
            @Override
            public void onStart(String srcFile, String targetFile, long totalBytes) {
                System.out.printf("Start uploading file '%s' to '%s' (%d bytes)\n", srcFile, targetFile, totalBytes);
            }

            @Override
            public void onProgress(String srcFile, String targetFile, long bytesUploaded, long totalBytes) {
                int length = 50;
                int p = (int)((length * bytesUploaded) / totalBytes);
                String progress = StringUtils.repeat("=", p);
                String todo = StringUtils.repeat(" ", length - p);

                System.out.printf("\r[%s%s] %d / %d", progress, todo, bytesUploaded, totalBytes);
            }

            @Override
            public void onCompleted(String srcFile, String targetFile, long totalBytes) {
                System.out.printf("\nFile '%s' uploaded to '%s' (%d bytes)\n", srcFile, targetFile, totalBytes);
            }
        });
    }

    private static boolean topologyNameExists(Map conf, String name, String asUser) {
        NimbusClient client = NimbusClient.getConfiguredClientAs(conf, asUser);
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

    private static String submitJar(Map conf, ProgressListener listener) {
        return  submitJar(conf, System.getProperty("storm.jar"), listener);
    }

    /**
     * Submit jar file
     * @param conf the topology-specific configuration. See {@link Config}.
     * @param localJar file path of the jar file to submit
     * @return the remote location of the submitted jar
     */
    public static String submitJar(Map conf, String localJar) {
        return submitJar(conf, localJar, null);
    }


    public static String submitJarAs(Map conf, String localJar, ProgressListener listener, String asUser) {
        if (localJar == null) {
            throw new RuntimeException("Must submit topologies using the 'storm' client script so that StormSubmitter knows which jar to upload.");
        }

        NimbusClient client = NimbusClient.getConfiguredClientAs(conf, asUser);
        try {
            String uploadLocation = client.getClient().beginFileUpload();
            LOG.info("Uploading topology jar " + localJar + " to assigned location: " + uploadLocation);
            BufferFileInputStream is = new BufferFileInputStream(localJar, THRIFT_CHUNK_SIZE_BYTES);

            long totalSize = new File(localJar).length();
            if (listener != null) {
                listener.onStart(localJar, uploadLocation, totalSize);
            }

            long bytesUploaded = 0;
            while(true) {
                byte[] toSubmit = is.read();
                bytesUploaded += toSubmit.length;
                if (listener != null) {
                    listener.onProgress(localJar, uploadLocation, bytesUploaded, totalSize);
                }

                if(toSubmit.length==0) break;
                client.getClient().uploadChunk(uploadLocation, ByteBuffer.wrap(toSubmit));
            }
            client.getClient().finishFileUpload(uploadLocation);

            if (listener != null) {
                listener.onCompleted(localJar, uploadLocation, totalSize);
            }

            LOG.info("Successfully uploaded topology jar to assigned location: " + uploadLocation);
            return uploadLocation;
        } catch(Exception e) {
            throw new RuntimeException(e);
        } finally {
            client.close();
        }
    }

    /**
     * Submit jar file
     * @param conf the topology-specific configuration. See {@link Config}.
     * @param localJar file path of the jar file to submit
     * @param listener progress listener to track the jar file upload
     * @return the remote location of the submitted jar
     */
    public static String submitJar(Map conf, String localJar, ProgressListener listener) {
        return submitJarAs(conf,localJar, listener, null);
    }

    /**
     * Interface use to track progress of file upload
     */
    public interface ProgressListener {
        /**
         * called before file is uploaded
         * @param srcFile - jar file to be uploaded
         * @param targetFile - destination file
         * @param totalBytes - total number of bytes of the file
         */
        public void onStart(String srcFile, String targetFile, long totalBytes);

        /**
         * called whenever a chunk of bytes is uploaded
         * @param srcFile - jar file to be uploaded
         * @param targetFile - destination file
         * @param bytesUploaded - number of bytes transferred so far
         * @param totalBytes - total number of bytes of the file
         */
        public void onProgress(String srcFile, String targetFile, long bytesUploaded, long totalBytes);

        /**
         * called when the file is uploaded
         * @param srcFile - jar file to be uploaded
         * @param targetFile - destination file
         * @param totalBytes - total number of bytes of the file
         */
        public void onCompleted(String srcFile, String targetFile, long totalBytes);
    }
}
