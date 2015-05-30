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
package backtype.storm.utils;

import backtype.storm.Config;
import backtype.storm.generated.AuthorizationException;
import backtype.storm.generated.ComponentCommon;
import backtype.storm.generated.ComponentObject;
import backtype.storm.generated.StormTopology;
import backtype.storm.serialization.DefaultSerializationDelegate;
import backtype.storm.serialization.SerializationDelegate;
import clojure.lang.IFn;
import clojure.lang.RT;
import org.apache.commons.lang.StringUtils;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.thrift.TException;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Id;
import org.json.simple.JSONValue;
import org.json.simple.parser.ParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.constructor.SafeConstructor;

import java.io.*;
import java.net.URL;
import java.net.URLDecoder;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.WritableByteChannel;
import java.io.File;
import java.io.FileInputStream;
import java.util.*;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

public class Utils {
    private static final Logger LOG = LoggerFactory.getLogger(Utils.class);
    public static final String DEFAULT_STREAM_ID = "default";

    private static SerializationDelegate serializationDelegate;

    static {
        Map conf = readStormConfig();
        serializationDelegate = getSerializationDelegate(conf);
    }

    public static Object newInstance(String klass) {
        try {
            Class c = Class.forName(klass);
            return c.newInstance();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public static byte[] serialize(Object obj) {
        return serializationDelegate.serialize(obj);
    }

    public static <T> T deserialize(byte[] serialized, Class<T> clazz) {
        return serializationDelegate.deserialize(serialized, clazz);
    }

    public static byte[] javaSerialize(Object obj) {
        try {
            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            ObjectOutputStream oos = new ObjectOutputStream(bos);
            oos.writeObject(obj);
            oos.close();
            return bos.toByteArray();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static <T> T javaDeserialize(byte[] serialized, Class<T> clazz) {
        try {
            ByteArrayInputStream bis = new ByteArrayInputStream(serialized);
            ObjectInputStream ois = new ObjectInputStream(bis);
            Object ret = ois.readObject();
            ois.close();
            return (T)ret;
        } catch(IOException ioe) {
            throw new RuntimeException(ioe);
        } catch(ClassNotFoundException e) {
            throw new RuntimeException(e);
        }
    }

    public static byte[] gzip(byte[] data) {
        try {
            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            GZIPOutputStream out = new GZIPOutputStream(bos);
            out.write(data);
            out.close();
            return bos.toByteArray();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static byte[] gunzip(byte[] data) {
        try {
            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            ByteArrayInputStream bis = new ByteArrayInputStream(data);
            GZIPInputStream in = new GZIPInputStream(bis);
            byte[] buffer = new byte[1024];
            int len = 0;
            while ((len = in.read(buffer)) >= 0) {
                bos.write(buffer, 0, len);
            }
            in.close();
            bos.close();
            return bos.toByteArray();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static <T> String join(Iterable<T> coll, String sep) {
        Iterator<T> it = coll.iterator();
        String ret = "";
        while(it.hasNext()) {
            ret = ret + it.next();
            if(it.hasNext()) {
                ret = ret + sep;
            }
        }
        return ret;
    }

    public static void sleep(long millis) {
        try {
            Time.sleep(millis);
        } catch(InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    public static List<URL> findResources(String name) {
        try {
            Enumeration<URL> resources = Thread.currentThread().getContextClassLoader().getResources(name);
            List<URL> ret = new ArrayList<URL>();
            while(resources.hasMoreElements()) {
                ret.add(resources.nextElement());
            }
            return ret;
        } catch(IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static Map findAndReadConfigFile(String name, boolean mustExist) {
        InputStream in = null;
        boolean confFileEmpty = false;
        try {
            in = getConfigFileInputStream(name);
            if (null != in) {
                Yaml yaml = new Yaml(new SafeConstructor());
                Map ret = (Map) yaml.load(new InputStreamReader(in));
                if (null != ret) {
                    return new HashMap(ret);
                } else {
                    confFileEmpty = true;
                }
            }

            if (mustExist) {
                if(confFileEmpty)
                    throw new RuntimeException("Config file " + name + " doesn't have any valid storm configs");
                else
                    throw new RuntimeException("Could not find config file on classpath " + name);
            } else {
                return new HashMap();
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        } finally {
            if (null != in) {
                try {
                    in.close();
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
        }
    }

    private static InputStream getConfigFileInputStream(String configFilePath)
            throws IOException {
        if (null == configFilePath) {
            throw new IOException(
                    "Could not find config file, name not specified");
        }

        HashSet<URL> resources = new HashSet<URL>(findResources(configFilePath));
        if (resources.isEmpty()) {
            File configFile = new File(configFilePath);
            if (configFile.exists()) {
                return new FileInputStream(configFile);
            }
        } else if (resources.size() > 1) {
            throw new IOException(
                    "Found multiple " + configFilePath
                            + " resources. You're probably bundling the Storm jars with your topology jar. "
                            + resources);
        } else {
            LOG.info("Using "+configFilePath+" from resources");
            URL resource = resources.iterator().next();
            return resource.openStream();
        }
        return null;
    }


    public static Map findAndReadConfigFile(String name) {
       return findAndReadConfigFile(name, true);
    }

    public static Map readDefaultConfig() {
        return findAndReadConfigFile("defaults.yaml", true);
    }

    public static Map readCommandLineOpts() {
        Map ret = new HashMap();
        String commandOptions = System.getProperty("storm.options");
        if(commandOptions != null) {
            String[] configs = commandOptions.split(",");
            for (String config : configs) {
                config = URLDecoder.decode(config);
                String[] options = config.split("=", 2);
                if (options.length == 2) {
                    Object val = options[1];
                    try {
                        val = JSONValue.parseWithException(options[1]);
                    } catch (ParseException ignored) {
                        //fall back to string, which is already set
                    }
                    ret.put(options[0], val);
                }
            }
        }
        return ret;
    }

    public static Map readStormConfig() {
        Map ret = readDefaultConfig();
        String confFile = System.getProperty("storm.conf.file");
        Map storm;
        if (confFile==null || confFile.equals("")) {
            storm = findAndReadConfigFile("storm.yaml", false);
        } else {
            storm = findAndReadConfigFile(confFile, true);
        }
        ret.putAll(storm);
        ret.putAll(readCommandLineOpts());
        return ret;
    }

    private static Object normalizeConf(Object conf) {
        if(conf==null) return new HashMap();
        if(conf instanceof Map) {
            Map confMap = new HashMap((Map) conf);
            for(Object key: confMap.keySet()) {
                Object val = confMap.get(key);
                confMap.put(key, normalizeConf(val));
            }
            return confMap;
        } else if(conf instanceof List) {
            List confList =  new ArrayList((List) conf);
            for(int i=0; i<confList.size(); i++) {
                Object val = confList.get(i);
                confList.set(i, normalizeConf(val));
            }
            return confList;
        } else if (conf instanceof Integer) {
            return ((Integer) conf).longValue();
        } else if(conf instanceof Float) {
            return ((Float) conf).doubleValue();
        } else {
            return conf;
        }
    }

    public static boolean isValidConf(Map<String, Object> stormConf) {
        return normalizeConf(stormConf).equals(normalizeConf((Map) JSONValue.parse(JSONValue.toJSONString(stormConf))));
    }

    public static Object getSetComponentObject(ComponentObject obj) {
        if(obj.getSetField()==ComponentObject._Fields.SERIALIZED_JAVA) {
            return Utils.javaDeserialize(obj.get_serialized_java(), Serializable.class);
        } else if(obj.getSetField()==ComponentObject._Fields.JAVA_OBJECT) {
            return obj.get_java_object();
        } else {
            return obj.get_shell();
        }
    }

    public static <S, T> T get(Map<S, T> m, S key, T def) {
        T ret = m.get(key);
        if(ret==null) {
            ret = def;
        }
        return ret;
    }

    public static List<Object> tuple(Object... values) {
        List<Object> ret = new ArrayList<Object>();
        for(Object v: values) {
            ret.add(v);
        }
        return ret;
    }

    public static void downloadFromMaster(Map conf, String file, String localFile) throws AuthorizationException, IOException, TException {
        NimbusClient client = NimbusClient.getConfiguredClient(conf);
        String id = client.getClient().beginFileDownload(file);
        WritableByteChannel out = Channels.newChannel(new FileOutputStream(localFile));
        while(true) {
            ByteBuffer chunk = client.getClient().downloadChunk(id);
            int written = out.write(chunk);
            if(written==0) break;
        }
        out.close();
    }

    public static IFn loadClojureFn(String namespace, String name) {
        try {
          clojure.lang.Compiler.eval(RT.readString("(require '" + namespace + ")"));
        } catch (Exception e) {
          //if playing from the repl and defining functions, file won't exist
        }
        return (IFn) RT.var(namespace, name).deref();
    }

    public static boolean isSystemId(String id) {
        return id.startsWith("__");
    }

    public static <K, V> Map<V, K> reverseMap(Map<K, V> map) {
        Map<V, K> ret = new HashMap<V, K>();
        for(K key: map.keySet()) {
            ret.put(map.get(key), key);
        }
        return ret;
    }

    public static ComponentCommon getComponentCommon(StormTopology topology, String id) {
        if(topology.get_spouts().containsKey(id)) {
            return topology.get_spouts().get(id).get_common();
        }
        if(topology.get_bolts().containsKey(id)) {
            return topology.get_bolts().get(id).get_common();
        }
        if(topology.get_state_spouts().containsKey(id)) {
            return topology.get_state_spouts().get(id).get_common();
        }
        throw new IllegalArgumentException("Could not find component with id " + id);
    }

    public static Integer getInt(Object o) {
      Integer result = getInt(o, null);
      if (null == result) {
        throw new IllegalArgumentException("Don't know how to convert null to int");
      }
      return result;
    }

    public static Integer getInt(Object o, Integer defaultValue) {
      if (null == o) {
        return defaultValue;
      }

      if (o instanceof Integer ||
          o instanceof Short ||
          o instanceof Byte) {
          return ((Number) o).intValue();
      } else if (o instanceof Long) {
          final long l = (Long) o;
          if (l <= Integer.MAX_VALUE && l >= Integer.MIN_VALUE) {
              return (int) l;
          }
      } else if (o instanceof String) {
          return Integer.parseInt((String) o);
      }

      throw new IllegalArgumentException("Don't know how to convert " + o + " to int");
    }

    public static boolean getBoolean(Object o, boolean defaultValue) {
      if (null == o) {
        return defaultValue;
      }

      if(o instanceof Boolean) {
          return (Boolean) o;
      } else {
          throw new IllegalArgumentException("Don't know how to convert " + o + " + to boolean");
      }
    }

    public static long secureRandomLong() {
        return UUID.randomUUID().getLeastSignificantBits();
    }

    public static CuratorFramework newCurator(Map conf, List<String> servers, Object port, String root) {
        return newCurator(conf, servers, port, root, null);
    }

    public static CuratorFramework newCurator(Map conf, List<String> servers, Object port, String root, ZookeeperAuthInfo auth) {
        List<String> serverPorts = new ArrayList<String>();
        for(String zkServer: (List<String>) servers) {
            serverPorts.add(zkServer + ":" + Utils.getInt(port));
        }
        String zkStr = StringUtils.join(serverPorts, ",") + root;
        CuratorFrameworkFactory.Builder builder = CuratorFrameworkFactory.builder();

        setupBuilder(builder, zkStr, conf, auth);

        return builder.build();
    }

    protected static void setupBuilder(CuratorFrameworkFactory.Builder builder, String zkStr, Map conf, ZookeeperAuthInfo auth)
    {
        builder.connectString(zkStr)
            .connectionTimeoutMs(Utils.getInt(conf.get(Config.STORM_ZOOKEEPER_CONNECTION_TIMEOUT)))
            .sessionTimeoutMs(Utils.getInt(conf.get(Config.STORM_ZOOKEEPER_SESSION_TIMEOUT)))
            .retryPolicy(new StormBoundedExponentialBackoffRetry(
                        Utils.getInt(conf.get(Config.STORM_ZOOKEEPER_RETRY_INTERVAL)),
                        Utils.getInt(conf.get(Config.STORM_ZOOKEEPER_RETRY_INTERVAL_CEILING)),
                        Utils.getInt(conf.get(Config.STORM_ZOOKEEPER_RETRY_TIMES))));

        if(auth!=null && auth.scheme!=null && auth.payload!=null) {
            builder = builder.authorization(auth.scheme, auth.payload);
        }
    }

    public static CuratorFramework newCurator(Map conf, List<String> servers, Object port, ZookeeperAuthInfo auth) {
        return newCurator(conf, servers, port, "", auth);
    }

    public static CuratorFramework newCuratorStarted(Map conf, List<String> servers, Object port, String root, ZookeeperAuthInfo auth) {
        CuratorFramework ret = newCurator(conf, servers, port, root, auth);
        ret.start();
        return ret;
    }

    public static CuratorFramework newCuratorStarted(Map conf, List<String> servers, Object port, ZookeeperAuthInfo auth) {
        CuratorFramework ret = newCurator(conf, servers, port, auth);
        ret.start();
        return ret;
    }

    /**
     *
(defn integer-divided [sum num-pieces]
  (let [base (int (/ sum num-pieces))
        num-inc (mod sum num-pieces)
        num-bases (- num-pieces num-inc)]
    (if (= num-inc 0)
      {base num-bases}
      {base num-bases (inc base) num-inc}
      )))
     * @param sum
     * @param numPieces
     * @return
     */

    public static TreeMap<Integer, Integer> integerDivided(int sum, int numPieces) {
        int base = sum / numPieces;
        int numInc = sum % numPieces;
        int numBases = numPieces - numInc;
        TreeMap<Integer, Integer> ret = new TreeMap<Integer, Integer>();
        ret.put(base, numBases);
        if(numInc!=0) {
            ret.put(base+1, numInc);
        }
        return ret;
    }

    public static byte[] toByteArray(ByteBuffer buffer) {
        byte[] ret = new byte[buffer.remaining()];
        buffer.get(ret, 0, ret.length);
        return ret;
    }

    public static void readAndLogStream(String prefix, InputStream in) {
        try {
            BufferedReader r = new BufferedReader(new InputStreamReader(in));
            String line = null;
            while ((line = r.readLine())!= null) {
                LOG.info("{}:{}", prefix, line);
            }
        } catch (IOException e) {
            LOG.warn("Error whiel trying to log stream", e);
        }
    }

    public static boolean exceptionCauseIsInstanceOf(Class klass, Throwable throwable) {
        Throwable t = throwable;
        while(t != null) {
            if(klass.isInstance(t)) {
                return true;
            }
            t = t.getCause();
        }
        return false;
    }

    /**
     * Is the cluster configured to interact with ZooKeeper in a secure way?
     * This only works when called from within Nimbus or a Supervisor process.
     * @param conf the storm configuration, not the topology configuration
     * @return true if it is configured else false.
     */
    public static boolean isZkAuthenticationConfiguredStormServer(Map conf) {
        return null != System.getProperty("java.security.auth.login.config")
            || (conf != null
                && conf.get(Config.STORM_ZOOKEEPER_AUTH_SCHEME) != null
                && ! ((String)conf.get(Config.STORM_ZOOKEEPER_AUTH_SCHEME)).isEmpty());
    }

    /**
     * Is the topology configured to have ZooKeeper authentication.
     * @param conf the topology configuration
     * @return true if ZK is configured else false
     */
    public static boolean isZkAuthenticationConfiguredTopology(Map conf) {
        return (conf != null
                && conf.get(Config.STORM_ZOOKEEPER_TOPOLOGY_AUTH_SCHEME) != null
                && ! ((String)conf.get(Config.STORM_ZOOKEEPER_TOPOLOGY_AUTH_SCHEME)).isEmpty());
    }

    public static List<ACL> getWorkerACL(Map conf) {
        //This is a work around to an issue with ZK where a sasl super user is not super unless there is an open SASL ACL so we are trying to give the correct perms
        if (!isZkAuthenticationConfiguredTopology(conf)) {
            return null;
        }
        String stormZKUser = (String)conf.get(Config.STORM_ZOOKEEPER_SUPERACL);
        if (stormZKUser == null) {
           throw new IllegalArgumentException("Authentication is enabled but "+Config.STORM_ZOOKEEPER_SUPERACL+" is not set");
        }
        String[] split = stormZKUser.split(":",2);
        if (split.length != 2) {
          throw new IllegalArgumentException(Config.STORM_ZOOKEEPER_SUPERACL+" does not appear to be in the form scheme:acl, i.e. sasl:storm-user");
        }
        ArrayList<ACL> ret = new ArrayList<ACL>(ZooDefs.Ids.CREATOR_ALL_ACL);
        ret.add(new ACL(ZooDefs.Perms.ALL, new Id(split[0], split[1])));
        return ret;
    }

   public static String threadDump() {
       final StringBuilder dump = new StringBuilder();
       final java.lang.management.ThreadMXBean threadMXBean =  java.lang.management.ManagementFactory.getThreadMXBean();
       final java.lang.management.ThreadInfo[] threadInfos = threadMXBean.getThreadInfo(threadMXBean.getAllThreadIds(), 100);
       for (java.lang.management.ThreadInfo threadInfo : threadInfos) {
           dump.append('"');
           dump.append(threadInfo.getThreadName());
           dump.append("\" ");
           final Thread.State state = threadInfo.getThreadState();
           dump.append("\n   java.lang.Thread.State: ");
           dump.append(state);
           final StackTraceElement[] stackTraceElements = threadInfo.getStackTrace();
           for (final StackTraceElement stackTraceElement : stackTraceElements) {
               dump.append("\n        at ");
               dump.append(stackTraceElement);
           }
           dump.append("\n\n");
       }
       return dump.toString();
   }

    // Assumes caller is synchronizing
    private static SerializationDelegate getSerializationDelegate(Map stormConf) {
        String delegateClassName = (String)stormConf.get(Config.STORM_META_SERIALIZATION_DELEGATE);
        SerializationDelegate delegate;
        try {
            Class delegateClass = Class.forName(delegateClassName);
            delegate = (SerializationDelegate) delegateClass.newInstance();
        } catch (ClassNotFoundException e) {
            LOG.error("Failed to construct serialization delegate, falling back to default", e);
            delegate = new DefaultSerializationDelegate();
        } catch (InstantiationException e) {
            LOG.error("Failed to construct serialization delegate, falling back to default", e);
            delegate = new DefaultSerializationDelegate();
        } catch (IllegalAccessException e) {
            LOG.error("Failed to construct serialization delegate, falling back to default", e);
            delegate = new DefaultSerializationDelegate();
        }
        delegate.prepare(stormConf);
        return delegate;
    }

  public static void handleUncaughtException(Throwable t) {
    if (t != null && t instanceof Error) {
      if (t instanceof OutOfMemoryError) {
        try {
          System.err.println("Halting due to Out Of Memory Error..." + Thread.currentThread().getName());
        } catch (Throwable err) {
          //Again we don't want to exit because of logging issues.
        }
        Runtime.getRuntime().halt(-1);
      } else {
        //Running in daemon mode, we would pass Error to calling thread.
        throw (Error) t;
      }
    }
  }
}

