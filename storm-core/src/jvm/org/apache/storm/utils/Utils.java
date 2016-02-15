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
package org.apache.storm.utils;

import org.apache.commons.exec.CommandLine;
import org.apache.commons.exec.DefaultExecutor;
import org.apache.commons.exec.ExecuteException;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.storm.Config;
import org.apache.storm.blobstore.BlobStore;
import org.apache.storm.blobstore.BlobStoreAclHandler;
import org.apache.storm.blobstore.ClientBlobStore;
import org.apache.storm.blobstore.InputStreamWithMeta;
import org.apache.storm.blobstore.LocalFsBlobStore;
import org.apache.storm.daemon.JarTransformer;
import org.apache.storm.generated.*;
import org.apache.storm.localizer.Localizer;
import org.apache.storm.nimbus.NimbusInfo;
import org.apache.storm.serialization.DefaultSerializationDelegate;
import org.apache.storm.serialization.SerializationDelegate;
import clojure.lang.RT;
import com.google.common.annotations.VisibleForTesting;
import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import org.apache.commons.io.input.ClassLoaderObjectInputStream;
import org.apache.commons.lang.StringUtils;
import org.apache.curator.ensemble.exhibitor.DefaultExhibitorRestClient;
import org.apache.curator.ensemble.exhibitor.ExhibitorEnsembleProvider;
import org.apache.curator.ensemble.exhibitor.Exhibitors;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.thrift.TBase;
import org.apache.thrift.TDeserializer;
import org.apache.thrift.TException;
import org.apache.thrift.TSerializer;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Id;
import org.eclipse.jetty.util.log.Log;
import org.json.simple.JSONValue;
import org.json.simple.parser.ParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.constructor.SafeConstructor;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.PrintStream;
import java.io.RandomAccessFile;
import java.io.Serializable;
import java.lang.management.ManagementFactory;
import java.net.InetAddress;
import java.net.URL;
import java.net.URLDecoder;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.PosixFilePermission;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;

public class Utils {
    // A singleton instance allows us to mock delegated static methods in our
    // tests by subclassing.
    private static Utils _instance = new Utils();

    /**
     * Provide an instance of this class for delegates to use.  To mock out
     * delegated methods, provide an instance of a subclass that overrides the
     * implementation of the delegated method.
     * @param u a Utils instance
     * @return the previously set instance
     */
    public static Utils setInstance(Utils u) {
        Utils oldInstance = _instance;
        _instance = u;
        return oldInstance;
    }

    private static final Logger LOG = LoggerFactory.getLogger(Utils.class);
    public static final String DEFAULT_STREAM_ID = "default";
    public static final String DEFAULT_BLOB_VERSION_SUFFIX = ".version";
    public static final String CURRENT_BLOB_SUFFIX_ID = "current";
    public static final String DEFAULT_CURRENT_BLOB_SUFFIX = "." + CURRENT_BLOB_SUFFIX_ID;
    private static ThreadLocal<TSerializer> threadSer = new ThreadLocal<TSerializer>();
    private static ThreadLocal<TDeserializer> threadDes = new ThreadLocal<TDeserializer>();

    private static SerializationDelegate serializationDelegate;
    private static ClassLoader cl = ClassLoader.getSystemClassLoader();

    public static final boolean IS_ON_WINDOWS = "Windows_NT".equals(System.getenv("OS"));
    public static final String FILE_PATH_SEPARATOR = System.getProperty("file.separator");
    public static final String CLASS_PATH_SEPARATOR = System.getProperty("path.separator");

    public static final int SIGKILL = 9;
    public static final int SIGTERM = 15;

    static {
        Map conf = readStormConfig();
        serializationDelegate = getSerializationDelegate(conf);
    }

    public static <T> T newInstance(String klass) {
        try {
            return newInstance((Class<T>)Class.forName(klass));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public static <T> T newInstance(Class<T> klass) {
        return _instance.newInstanceImpl(klass);
    }

    // Non-static impl methods exist for mocking purposes.
    public <T> T newInstanceImpl(Class<T> klass) {
        try {
            return klass.newInstance();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public static JarTransformer jarTransformer(String klass) {
        JarTransformer ret = null;
        if (klass != null) {
            ret = (JarTransformer)newInstance(klass);
        }
        return ret;
    }

    public static byte[] serialize(Object obj) {
        return serializationDelegate.serialize(obj);
    }

    public static <T> T deserialize(byte[] serialized, Class<T> clazz) {
        return serializationDelegate.deserialize(serialized, clazz);
    }

    public static <T> T thriftDeserialize(Class c, byte[] b, int offset, int length) {
        try {
            T ret = (T) c.newInstance();
            TDeserializer des = getDes();
            des.deserialize((TBase)ret, b, offset, length);
            return ret;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
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
            ObjectInputStream ois = new ClassLoaderObjectInputStream(cl, bis);
            Object ret = ois.readObject();
            ois.close();
            return (T)ret;
        } catch (IOException ioe) {
            throw new RuntimeException(ioe);
        } catch (ClassNotFoundException e) {
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

    public static byte[] toCompressedJsonConf(Map<String, Object> stormConf) {
        try {
            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            OutputStreamWriter out = new OutputStreamWriter(new GZIPOutputStream(bos));
            JSONValue.writeJSONString(stormConf, out);
            out.close();
            return bos.toByteArray();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static Map<String, Object> fromCompressedJsonConf(byte[] serialized) {
        try {
            ByteArrayInputStream bis = new ByteArrayInputStream(serialized);
            InputStreamReader in = new InputStreamReader(new GZIPInputStream(bis));
            Object ret = JSONValue.parseWithException(in);
            in.close();
            return (Map<String,Object>)ret;
        } catch (IOException ioe) {
            throw new RuntimeException(ioe);
        } catch (ParseException e) {
            throw new RuntimeException(e);
        }
    }

    public static <T> String join(Iterable<T> coll, String sep) {
        Iterator<T> it = coll.iterator();
        StringBuilder ret = new StringBuilder();
        while(it.hasNext()) {
            ret.append(it.next());
            if(it.hasNext()) {
                ret.append(sep);
            }
        }
        return ret.toString();
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
            while (resources.hasMoreElements()) {
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
            LOG.debug("Using "+configFilePath+" from resources");
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
        if (commandOptions != null) {
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
        if (confFile == null || confFile.equals("")) {
            storm = findAndReadConfigFile("storm.yaml", false);
        } else {
            storm = findAndReadConfigFile(confFile, true);
        }
        ret.putAll(storm);
        ret.putAll(readCommandLineOpts());
        return ret;
    }

    private static Object normalizeConf(Object conf) {
        if (conf == null) return new HashMap();
        if (conf instanceof Map) {
            Map<Object, Object> confMap = new HashMap((Map) conf);
            for (Map.Entry<Object, Object> entry : confMap.entrySet()) {
                confMap.put(entry.getKey(), normalizeConf(entry.getValue()));
            }
            return confMap;
        } else if (conf instanceof List) {
            List confList =  new ArrayList((List) conf);
            for (int i = 0; i < confList.size(); i++) {
                Object val = confList.get(i);
                confList.set(i, normalizeConf(val));
            }
            return confList;
        } else if (conf instanceof Integer) {
            return ((Integer) conf).longValue();
        } else if (conf instanceof Float) {
            return ((Float) conf).doubleValue();
        } else {
            return conf;
        }
    }

    public static boolean isValidConf(Map<String, Object> stormConf) {
        return normalizeConf(stormConf).equals(normalizeConf((Map) JSONValue.parse(JSONValue.toJSONString(stormConf))));
    }

    public static Object getSetComponentObject(ComponentObject obj) {
        if (obj.getSetField() == ComponentObject._Fields.SERIALIZED_JAVA) {
            return Utils.javaDeserialize(obj.get_serialized_java(), Serializable.class);
        } else if (obj.getSetField() == ComponentObject._Fields.JAVA_OBJECT) {
            return obj.get_java_object();
        } else {
            return obj.get_shell();
        }
    }

    public static <S, T> T get(Map<S, T> m, S key, T def) {
        T ret = m.get(key);
        if (ret == null) {
            ret = def;
        }
        return ret;
    }

    public static List<Object> tuple(Object... values) {
        List<Object> ret = new ArrayList<Object>();
        for (Object v : values) {
            ret.add(v);
        }
        return ret;
    }


    public static Localizer createLocalizer(Map conf, String baseDir) {
        return new Localizer(conf, baseDir);
    }

    public static ClientBlobStore getClientBlobStoreForSupervisor(Map conf) {
        ClientBlobStore store = (ClientBlobStore) newInstance(
                (String) conf.get(Config.SUPERVISOR_BLOBSTORE));
        store.prepare(conf);
        return store;
    }

    public static BlobStore getNimbusBlobStore(Map conf, NimbusInfo nimbusInfo) {
        return getNimbusBlobStore(conf, null, nimbusInfo);
    }

    public static BlobStore getNimbusBlobStore(Map conf, String baseDir, NimbusInfo nimbusInfo) {
        String type = (String)conf.get(Config.NIMBUS_BLOBSTORE);
        if (type == null) {
            type = LocalFsBlobStore.class.getName();
        }
        BlobStore store = (BlobStore) newInstance(type);
        HashMap nconf = new HashMap(conf);
        // only enable cleanup of blobstore on nimbus
        nconf.put(Config.BLOBSTORE_CLEANUP_ENABLE, Boolean.TRUE);

        if(store != null) {
            // store can be null during testing when mocking utils.
            store.prepare(nconf, baseDir, nimbusInfo);
        }
        return store;
    }

    /**
     * Meant to be called only by the supervisor for stormjar/stormconf/stormcode files.
     * @param key
     * @param localFile
     * @param cb
     * @throws AuthorizationException
     * @throws KeyNotFoundException
     * @throws IOException
     */
    public static void downloadResourcesAsSupervisor(String key, String localFile,
                                                     ClientBlobStore cb) throws AuthorizationException, KeyNotFoundException, IOException {
        final int MAX_RETRY_ATTEMPTS = 2;
        final int ATTEMPTS_INTERVAL_TIME = 100;
        for (int retryAttempts = 0; retryAttempts < MAX_RETRY_ATTEMPTS; retryAttempts++) {
            if (downloadResourcesAsSupervisorAttempt(cb, key, localFile)) {
                break;
            }
            Utils.sleep(ATTEMPTS_INTERVAL_TIME);
        }
    }

    public static ClientBlobStore getClientBlobStore(Map conf) {
        ClientBlobStore store = (ClientBlobStore) Utils.newInstance((String) conf.get(Config.CLIENT_BLOBSTORE));
        store.prepare(conf);
        return store;
    }

    private static boolean downloadResourcesAsSupervisorAttempt(ClientBlobStore cb, String key, String localFile) {
        boolean isSuccess = false;
        FileOutputStream out = null;
        InputStreamWithMeta in = null;
        try {
            out = new FileOutputStream(localFile);
            in = cb.getBlob(key);
            long fileSize = in.getFileLength();

            byte[] buffer = new byte[1024];
            int len;
            int downloadFileSize = 0;
            while ((len = in.read(buffer)) >= 0) {
                out.write(buffer, 0, len);
                downloadFileSize += len;
            }

            isSuccess = (fileSize == downloadFileSize);
        } catch (TException | IOException e) {
            LOG.error("An exception happened while downloading {} from blob store.", localFile, e);
        } finally {
            try {
                if (out != null) {
                    out.close();
                }
            } catch (IOException ignored) {}
            try {
                if (in != null) {
                    in.close();
                }
            } catch (IOException ignored) {}
        }
        if (!isSuccess) {
            try {
                Files.deleteIfExists(Paths.get(localFile));
            } catch (IOException ex) {
                LOG.error("Failed trying to delete the partially downloaded {}", localFile, ex);
            }
        }
        return isSuccess;
    }

    public static boolean checkFileExists(String path) {
        return Files.exists(new File(path).toPath());
    }

    public static boolean checkFileExists(String dir, String file) {
        return Files.exists(new File(dir, file).toPath());
    }

    public static long nimbusVersionOfBlob(String key, ClientBlobStore cb) throws AuthorizationException, KeyNotFoundException {
        long nimbusBlobVersion = 0;
        ReadableBlobMeta metadata = cb.getBlobMeta(key);
        nimbusBlobVersion = metadata.get_version();
        return nimbusBlobVersion;
    }

    public static String getFileOwner(String path) throws IOException {
        return Files.getOwner(FileSystems.getDefault().getPath(path)).getName();
    }

    public static long localVersionOfBlob(String localFile) {
        File f = new File(localFile + DEFAULT_BLOB_VERSION_SUFFIX);
        long currentVersion = 0;
        if (f.exists() && !(f.isDirectory())) {
            BufferedReader br = null;
            try {
                br = new BufferedReader(new FileReader(f));
                String line = br.readLine();
                currentVersion = Long.parseLong(line);
            } catch (IOException e) {
                throw new RuntimeException(e);
            } finally {
                try {
                    if (br != null) {
                        br.close();
                    }
                } catch (Exception ignore) {
                    LOG.error("Exception trying to cleanup", ignore);
                }
            }
            return currentVersion;
        } else {
            return -1;
        }
    }

    public static String constructBlobWithVersionFileName(String fileName, long version) {
        return fileName + "." + version;
    }

    public static String constructBlobCurrentSymlinkName(String fileName) {
        return fileName + Utils.DEFAULT_CURRENT_BLOB_SUFFIX;
    }

    public static String constructVersionFileName(String fileName) {
        return fileName + Utils.DEFAULT_BLOB_VERSION_SUFFIX;
    }
    // only works on operating  systems that support posix
    public static void restrictPermissions(String baseDir) {
        try {
            Set<PosixFilePermission> perms = new HashSet<PosixFilePermission>(
                    Arrays.asList(PosixFilePermission.OWNER_READ, PosixFilePermission.OWNER_WRITE,
                            PosixFilePermission.OWNER_EXECUTE, PosixFilePermission.GROUP_READ,
                            PosixFilePermission.GROUP_EXECUTE));
            Files.setPosixFilePermissions(FileSystems.getDefault().getPath(baseDir), perms);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }


    public static synchronized clojure.lang.IFn loadClojureFn(String namespace, String name) {
        try {
            clojure.lang.Compiler.eval(RT.readString("(require '" + namespace + ")"));
        } catch (Exception e) {
            //if playing from the repl and defining functions, file won't exist
        }
        return (clojure.lang.IFn) RT.var(namespace, name).deref();
    }

    public static boolean isSystemId(String id) {
        return id.startsWith("__");
    }

    public static ComponentCommon getComponentCommon(StormTopology topology, String id) {
        if (topology.get_spouts().containsKey(id)) {
            return topology.get_spouts().get(id).get_common();
        }
        if (topology.get_bolts().containsKey(id)) {
            return topology.get_bolts().get(id).get_common();
        }
        if (topology.get_state_spouts().containsKey(id)) {
            return topology.get_state_spouts().get(id).get_common();
        }
        throw new IllegalArgumentException("Could not find component with id " + id);
    }

    public static List<String> getStrings(final Object o) {
        if (o == null) {
            return new ArrayList<String>();
        } else if (o instanceof String) {
            return new ArrayList<String>() {{ add((String) o); }};
        } else if (o instanceof Collection) {
            List<String> answer = new ArrayList<String>();
            for (Object v : (Collection) o) {
                answer.add(v.toString());
            }
            return answer;
        } else {
            throw new IllegalArgumentException("Don't know how to convert to string list");
        }
    }

    public static String getString(Object o) {
        if (null == o) {
            throw new IllegalArgumentException("Don't know how to convert null to String");
        }
        return o.toString();
    }

    public static Integer getInt(Object o) {
        Integer result = getInt(o, null);
        if (null == result) {
            throw new IllegalArgumentException("Don't know how to convert null to int");
        }
        return result;
    }

    private static TDeserializer getDes() {
        TDeserializer des = threadDes.get();
        if(des == null) {
            des = new TDeserializer();
            threadDes.set(des);
        }
        return des;
    }

    public static byte[] thriftSerialize(TBase t) {
        try {
            TSerializer ser = threadSer.get();
            if (ser == null) {
                ser = new TSerializer();
                threadSer.set(ser);
            }
            return ser.serialize(t);
        } catch (TException e) {
            LOG.error("Failed to serialize to thrift: ", e);
            throw new RuntimeException(e);
        }
    }

    public static <T> T thriftDeserialize(Class c, byte[] b) {
        try {
            return Utils.thriftDeserialize(c, b, 0, b.length);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
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

    public static Double getDouble(Object o) {
        Double result = getDouble(o, null);
        if (null == result) {
            throw new IllegalArgumentException("Don't know how to convert null to double");
        }
        return result;
    }

    public static Double getDouble(Object o, Double defaultValue) {
        if (null == o) {
            return defaultValue;
        }
        if (o instanceof Number) {
            return ((Number) o).doubleValue();
        } else {
            throw new IllegalArgumentException("Don't know how to convert " + o + " + to double");
        }
    }

    public static boolean getBoolean(Object o, boolean defaultValue) {
        if (null == o) {
            return defaultValue;
        }
        if (o instanceof Boolean) {
            return (Boolean) o;
        } else {
            throw new IllegalArgumentException("Don't know how to convert " + o + " + to boolean");
        }
    }

    public static String getString(Object o, String defaultValue) {
        if (null == o) {
            return defaultValue;
        }
        if (o instanceof String) {
            return (String) o;
        } else {
            throw new IllegalArgumentException("Don't know how to convert " + o + " + to String");
        }
    }

    public static long secureRandomLong() {
        return UUID.randomUUID().getLeastSignificantBits();
    }

    /**
     * Unpack matching files from a jar. Entries inside the jar that do
     * not match the given pattern will be skipped.
     *
     * @param jarFile the .jar file to unpack
     * @param toDir the destination directory into which to unpack the jar
     */
    public static void unJar(File jarFile, File toDir)
            throws IOException {
        JarFile jar = new JarFile(jarFile);
        try {
            Enumeration<JarEntry> entries = jar.entries();
            while (entries.hasMoreElements()) {
                final JarEntry entry = entries.nextElement();
                if (!entry.isDirectory()) {
                    InputStream in = jar.getInputStream(entry);
                    try {
                        File file = new File(toDir, entry.getName());
                        ensureDirectory(file.getParentFile());
                        OutputStream out = new FileOutputStream(file);
                        try {
                            copyBytes(in, out, 8192);
                        } finally {
                            out.close();
                        }
                    } finally {
                        in.close();
                    }
                }
            }
        } finally {
            jar.close();
        }
    }

    /**
     * Copies from one stream to another.
     *
     * @param in InputStream to read from
     * @param out OutputStream to write to
     * @param buffSize the size of the buffer
     */
    public static void copyBytes(InputStream in, OutputStream out, int buffSize)
            throws IOException {
        PrintStream ps = out instanceof PrintStream ? (PrintStream)out : null;
        byte buf[] = new byte[buffSize];
        int bytesRead = in.read(buf);
        while (bytesRead >= 0) {
            out.write(buf, 0, bytesRead);
            if ((ps != null) && ps.checkError()) {
                throw new IOException("Unable to write to output stream.");
            }
            bytesRead = in.read(buf);
        }
    }

    /**
     * Ensure the existence of a given directory.
     *
     * @throws IOException if it cannot be created and does not already exist
     */
    private static void ensureDirectory(File dir) throws IOException {
        if (!dir.mkdirs() && !dir.isDirectory()) {
            throw new IOException("Mkdirs failed to create " +
                    dir.toString());
        }
    }

    /**
     * Given a Tar File as input it will untar the file in a the untar directory
     * passed as the second parameter
     * <p/>
     * This utility will untar ".tar" files and ".tar.gz","tgz" files.
     *
     * @param inFile   The tar file as input.
     * @param untarDir The untar directory where to untar the tar file.
     * @throws IOException
     */
    public static void unTar(File inFile, File untarDir) throws IOException {
        if (!untarDir.mkdirs()) {
            if (!untarDir.isDirectory()) {
                throw new IOException("Mkdirs failed to create " + untarDir);
            }
        }

        boolean gzipped = inFile.toString().endsWith("gz");
        if (isOnWindows()) {
            // Tar is not native to Windows. Use simple Java based implementation for
            // tests and simple tar archives
            unTarUsingJava(inFile, untarDir, gzipped);
        } else {
            // spawn tar utility to untar archive for full fledged unix behavior such
            // as resolving symlinks in tar archives
            unTarUsingTar(inFile, untarDir, gzipped);
        }
    }

    private static void unTarUsingTar(File inFile, File untarDir,
                                      boolean gzipped) throws IOException {
        StringBuffer untarCommand = new StringBuffer();
        if (gzipped) {
            untarCommand.append(" gzip -dc '");
            untarCommand.append(inFile.toString());
            untarCommand.append("' | (");
        }
        untarCommand.append("cd '");
        untarCommand.append(untarDir.toString());
        untarCommand.append("' ; ");
        untarCommand.append("tar -xf ");

        if (gzipped) {
            untarCommand.append(" -)");
        } else {
            untarCommand.append(inFile.toString());
        }
        String[] shellCmd = {"bash", "-c", untarCommand.toString()};
        ShellUtils.ShellCommandExecutor shexec = new ShellUtils.ShellCommandExecutor(shellCmd);
        shexec.execute();
        int exitcode = shexec.getExitCode();
        if (exitcode != 0) {
            throw new IOException("Error untarring file " + inFile +
                    ". Tar process exited with exit code " + exitcode);
        }
    }

    private static void unTarUsingJava(File inFile, File untarDir,
                                       boolean gzipped) throws IOException {
        InputStream inputStream = null;
        try {
            if (gzipped) {
                inputStream = new BufferedInputStream(new GZIPInputStream(
                        new FileInputStream(inFile)));
            } else {
                inputStream = new BufferedInputStream(new FileInputStream(inFile));
            }
            try (TarArchiveInputStream tis = new TarArchiveInputStream(inputStream)) {
                for (TarArchiveEntry entry = tis.getNextTarEntry(); entry != null; ) {
                    unpackEntries(tis, entry, untarDir);
                    entry = tis.getNextTarEntry();
                }
            }
        } finally {
            if(inputStream != null) {
                inputStream.close();
            }
        }
    }

    private static void unpackEntries(TarArchiveInputStream tis,
                                      TarArchiveEntry entry, File outputDir) throws IOException {
        if (entry.isDirectory()) {
            File subDir = new File(outputDir, entry.getName());
            if (!subDir.mkdirs() && !subDir.isDirectory()) {
                throw new IOException("Mkdirs failed to create tar internal dir "
                        + outputDir);
            }
            for (TarArchiveEntry e : entry.getDirectoryEntries()) {
                unpackEntries(tis, e, subDir);
            }
            return;
        }
        File outputFile = new File(outputDir, entry.getName());
        if (!outputFile.getParentFile().exists()) {
            if (!outputFile.getParentFile().mkdirs()) {
                throw new IOException("Mkdirs failed to create tar internal dir "
                                      + outputDir);
            }
        }
        int count;
        byte data[] = new byte[2048];
        BufferedOutputStream outputStream = new BufferedOutputStream(
                new FileOutputStream(outputFile));

        while ((count = tis.read(data)) != -1) {
            outputStream.write(data, 0, count);
        }
        outputStream.flush();
        outputStream.close();
    }

    public static boolean isOnWindows() {
        if (System.getenv("OS") != null) {
            return System.getenv("OS").equals("Windows_NT");
        }
        return false;
    }

    public static boolean isAbsolutePath(String path) {
        return Paths.get(path).isAbsolute();
    }

    public static void unpack(File localrsrc, File dst) throws IOException {
        String lowerDst = localrsrc.getName().toLowerCase();
        if (lowerDst.endsWith(".jar")) {
            unJar(localrsrc, dst);
        } else if (lowerDst.endsWith(".zip")) {
            unZip(localrsrc, dst);
        } else if (lowerDst.endsWith(".tar.gz") ||
                lowerDst.endsWith(".tgz") ||
                lowerDst.endsWith(".tar")) {
            unTar(localrsrc, dst);
        } else {
            LOG.warn("Cannot unpack " + localrsrc);
            if (!localrsrc.renameTo(dst)) {
                throw new IOException("Unable to rename file: [" + localrsrc
                        + "] to [" + dst + "]");
            }
        }
        if (localrsrc.isFile()) {
            localrsrc.delete();
        }
    }

    public static boolean canUserReadBlob(ReadableBlobMeta meta, String user) {
        SettableBlobMeta settable = meta.get_settable();
        for (AccessControl acl : settable.get_acl()) {
            if (acl.get_type().equals(AccessControlType.OTHER) && (acl.get_access() & BlobStoreAclHandler.READ) > 0) {
                return true;
            }
            if (acl.get_name().equals(user) && (acl.get_access() & BlobStoreAclHandler.READ) > 0) {
                return true;
            }
        }
        return false;
    }

    public static CuratorFramework newCurator(Map conf, List<String> servers, Object port, String root) {
        return newCurator(conf, servers, port, root, null);
    }

    public static CuratorFramework newCurator(Map conf, List<String> servers, Object port, String root, ZookeeperAuthInfo auth) {
        List<String> serverPorts = new ArrayList<String>();
        for (String zkServer : servers) {
            serverPorts.add(zkServer + ":" + Utils.getInt(port));
        }
        String zkStr = StringUtils.join(serverPorts, ",") + root;
        CuratorFrameworkFactory.Builder builder = CuratorFrameworkFactory.builder();

        setupBuilder(builder, zkStr, conf, auth);

        return builder.build();
    }

    protected static void setupBuilder(CuratorFrameworkFactory.Builder builder, final String zkStr, Map conf, ZookeeperAuthInfo auth)
    {
        List<String> exhibitorServers = getStrings(conf.get(Config.STORM_EXHIBITOR_SERVERS));
        if (!exhibitorServers.isEmpty()) {
            // use exhibitor servers
            builder.ensembleProvider(new ExhibitorEnsembleProvider(
                new Exhibitors(exhibitorServers, Utils.getInt(conf.get(Config.STORM_EXHIBITOR_PORT)),
                    new Exhibitors.BackupConnectionStringProvider() {
                        @Override
                        public String getBackupConnectionString() throws Exception {
                            // use zk servers as backup if they exist
                            return zkStr;
                        }}),
                new DefaultExhibitorRestClient(),
                Utils.getString(conf.get(Config.STORM_EXHIBITOR_URIPATH)),
                Utils.getInt(conf.get(Config.STORM_EXHIBITOR_POLL)),
                new StormBoundedExponentialBackoffRetry(
                    Utils.getInt(conf.get(Config.STORM_EXHIBITOR_RETRY_INTERVAL)),
                    Utils.getInt(conf.get(Config.STORM_EXHIBITOR_RETRY_INTERVAL_CEILING)),
                    Utils.getInt(conf.get(Config.STORM_EXHIBITOR_RETRY_TIMES)))));
        } else {
            builder.connectString(zkStr);
        }
        builder
            .connectionTimeoutMs(Utils.getInt(conf.get(Config.STORM_ZOOKEEPER_CONNECTION_TIMEOUT)))
            .sessionTimeoutMs(Utils.getInt(conf.get(Config.STORM_ZOOKEEPER_SESSION_TIMEOUT)))
            .retryPolicy(new StormBoundedExponentialBackoffRetry(
                    Utils.getInt(conf.get(Config.STORM_ZOOKEEPER_RETRY_INTERVAL)),
                    Utils.getInt(conf.get(Config.STORM_ZOOKEEPER_RETRY_INTERVAL_CEILING)),
                    Utils.getInt(conf.get(Config.STORM_ZOOKEEPER_RETRY_TIMES))));

        if (auth != null && auth.scheme != null && auth.payload != null) {
            builder.authorization(auth.scheme, auth.payload);
        }
    }

    public static void testSetupBuilder(CuratorFrameworkFactory.Builder
                                                builder, String zkStr, Map conf, ZookeeperAuthInfo auth)
    {
        setupBuilder(builder, zkStr, conf, auth);
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

    public static TreeMap<Integer, Integer> integerDivided(int sum, int numPieces) {
        int base = sum / numPieces;
        int numInc = sum % numPieces;
        int numBases = numPieces - numInc;
        TreeMap<Integer, Integer> ret = new TreeMap<Integer, Integer>();
        ret.put(base, numBases);
        if (numInc != 0) {
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
            while ((line = r.readLine()) != null) {
                LOG.info("{}:{}", prefix, line);
            }
        } catch (IOException e) {
            LOG.warn("Error while trying to log stream", e);
        }
    }

    /**
     * Checks if a throwable is an instance of a particular class
     * @param klass The class you're expecting
     * @param throwable The throwable you expect to be an instance of klass
     * @return true if throwable is instance of klass, false otherwise.
     */
    public static boolean exceptionCauseIsInstanceOf(Class klass, Throwable throwable) {
        Throwable t = throwable;
        while (t != null) {
            if (klass.isInstance(t)) {
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
                && !((String)conf.get(Config.STORM_ZOOKEEPER_AUTH_SCHEME)).isEmpty());
    }

    /**
     * Is the topology configured to have ZooKeeper authentication.
     * @param conf the topology configuration
     * @return true if ZK is configured else false
     */
    public static boolean isZkAuthenticationConfiguredTopology(Map conf) {
        return (conf != null
                && conf.get(Config.STORM_ZOOKEEPER_TOPOLOGY_AUTH_SCHEME) != null
                && !((String)conf.get(Config.STORM_ZOOKEEPER_TOPOLOGY_AUTH_SCHEME)).isEmpty());
    }


    public static List<ACL> getWorkerACL(Map conf) {
        //This is a work around to an issue with ZK where a sasl super user is not super unless there is an open SASL ACL so we are trying to give the correct perms
        if (!isZkAuthenticationConfiguredTopology(conf)) {
            return null;
        }
        String stormZKUser = (String)conf.get(Config.STORM_ZOOKEEPER_SUPERACL);
        if (stormZKUser == null) {
            throw new IllegalArgumentException("Authentication is enabled but " + Config.STORM_ZOOKEEPER_SUPERACL + " is not set");
        }
        String[] split = stormZKUser.split(":", 2);
        if (split.length != 2) {
            throw new IllegalArgumentException(Config.STORM_ZOOKEEPER_SUPERACL + " does not appear to be in the form scheme:acl, i.e. sasl:storm-user");
        }
        ArrayList<ACL> ret = new ArrayList<ACL>(ZooDefs.Ids.CREATOR_ALL_ACL);
        ret.add(new ACL(ZooDefs.Perms.ALL, new Id(split[0], split[1])));
        return ret;
    }

    /**
     * Takes an input dir or file and returns the disk usage on that local directory.
     * Very basic implementation.
     *
     * @param dir The input dir to get the disk space of this local dir
     * @return The total disk space of the input local directory
     */
    public static long getDU(File dir) {
        long size = 0;
        if (!dir.exists())
            return 0;
        if (!dir.isDirectory()) {
            return dir.length();
        } else {
            File[] allFiles = dir.listFiles();
            if(allFiles != null) {
                for (int i = 0; i < allFiles.length; i++) {
                    boolean isSymLink;
                    try {
                        isSymLink = org.apache.commons.io.FileUtils.isSymlink(allFiles[i]);
                    } catch(IOException ioe) {
                        isSymLink = true;
                    }
                    if(!isSymLink) {
                        size += getDU(allFiles[i]);
                    }
                }
            }
            return size;
        }
    }

    /**
     * Gets some information, including stack trace, for a running thread.
     * @return A human-readable string of the dump.
     */
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

    /**
     * Creates an instance of the pluggable SerializationDelegate or falls back to
     * DefaultSerializationDelegate if something goes wrong.
     * @param stormConf The config from which to pull the name of the pluggable class.
     * @return an instance of the class specified by storm.meta.serialization.delegate
     */
    private static SerializationDelegate getSerializationDelegate(Map stormConf) {
        String delegateClassName = (String)stormConf.get(Config.STORM_META_SERIALIZATION_DELEGATE);
        SerializationDelegate delegate;
        try {
            Class delegateClass = Class.forName(delegateClassName);
            delegate = (SerializationDelegate) delegateClass.newInstance();
        } catch (ClassNotFoundException | InstantiationException | IllegalAccessException e) {
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

    /**
     * Given a File input it will unzip the file in a the unzip directory
     * passed as the second parameter
     * @param inFile The zip file as input
     * @param unzipDir The unzip directory where to unzip the zip file.
     * @throws IOException
     */
    public static void unZip(File inFile, File unzipDir) throws IOException {
        Enumeration<? extends ZipEntry> entries;
        ZipFile zipFile = new ZipFile(inFile);

        try {
            entries = zipFile.entries();
            while (entries.hasMoreElements()) {
                ZipEntry entry = entries.nextElement();
                if (!entry.isDirectory()) {
                    InputStream in = zipFile.getInputStream(entry);
                    try {
                        File file = new File(unzipDir, entry.getName());
                        if (!file.getParentFile().mkdirs()) {
                            if (!file.getParentFile().isDirectory()) {
                                throw new IOException("Mkdirs failed to create " +
                                                      file.getParentFile().toString());
                            }
                        }
                        OutputStream out = new FileOutputStream(file);
                        try {
                            byte[] buffer = new byte[8192];
                            int i;
                            while ((i = in.read(buffer)) != -1) {
                                out.write(buffer, 0, i);
                            }
                        } finally {
                            out.close();
                        }
                    } finally {
                        in.close();
                    }
                }
            }
        } finally {
            zipFile.close();
        }
    }

    /**
     * Given a zip File input it will return its size
     * Only works for zip files whose uncompressed size is less than 4 GB,
     * otherwise returns the size module 2^32, per gzip specifications
     * @param myFile The zip file as input
     * @throws IOException
     * @return zip file size as a long
     */
    public static long zipFileSize(File myFile) throws IOException{
        RandomAccessFile raf = new RandomAccessFile(myFile, "r");
        raf.seek(raf.length() - 4);
        long b4 = raf.read();
        long b3 = raf.read();
        long b2 = raf.read();
        long b1 = raf.read();
        long val = (b1 << 24) | (b2 << 16) + (b3 << 8) + b4;
        raf.close();
        return val;
    }

    public static double zeroIfNaNOrInf(double x) {
        return (Double.isNaN(x) || Double.isInfinite(x)) ? 0.0 : x;
    }

    /**
     * parses the arguments to extract jvm heap memory size in MB.
     * @param input
     * @param defaultValue
     * @return the value of the JVM heap memory setting (in MB) in a java command.
     */
    public static Double parseJvmHeapMemByChildOpts(String input, Double defaultValue) {
        if (input != null) {
            Pattern optsPattern = Pattern.compile("Xmx[0-9]+[mkgMKG]");
            Matcher m = optsPattern.matcher(input);
            String memoryOpts = null;
            while (m.find()) {
                memoryOpts = m.group();
            }
            if (memoryOpts != null) {
                int unit = 1;
                if (memoryOpts.toLowerCase().endsWith("k")) {
                    unit = 1024;
                } else if (memoryOpts.toLowerCase().endsWith("m")) {
                    unit = 1024 * 1024;
                } else if (memoryOpts.toLowerCase().endsWith("g")) {
                    unit = 1024 * 1024 * 1024;
                }
                memoryOpts = memoryOpts.replaceAll("[a-zA-Z]", "");
                Double result =  Double.parseDouble(memoryOpts) * unit / 1024.0 / 1024.0;
                return (result < 1.0) ? 1.0 : result;
            } else {
                return defaultValue;
            }
        } else {
            return defaultValue;
        }
    }

    @VisibleForTesting
    public static void setClassLoaderForJavaDeSerialize(ClassLoader cl) {
        Utils.cl = cl;
    }

    @VisibleForTesting
    public static void resetClassLoaderForJavaDeSerialize() {
        Utils.cl = ClassLoader.getSystemClassLoader();
    }

    public static TopologyInfo getTopologyInfo(String name, String asUser, Map stormConf) {
        NimbusClient client = NimbusClient.getConfiguredClientAs(stormConf, asUser);
        TopologyInfo topologyInfo = null;
        try {
            ClusterSummary summary = client.getClient().getClusterInfo();
            for(TopologySummary s : summary.get_topologies()) {
                if(s.get_name().equals(name)) {
                    topologyInfo = client.getClient().getTopologyInfo(s.get_id());
                }
            }
        } catch(Exception e) {
            throw new RuntimeException(e);
        } finally {
            client.close();
        }
        return topologyInfo;
    }

    /**
     * A cheap way to deterministically convert a number to a positive value. When the input is
     * positive, the original value is returned. When the input number is negative, the returned
     * positive value is the original value bit AND against Integer.MAX_VALUE(0x7fffffff) which
     * is not its absolutely value.
     *
     * @param number a given number
     * @return a positive number.
     */
    public static int toPositive(int number) {
        return number & Integer.MAX_VALUE;
    }

    public static RuntimeException wrapInRuntime(Exception e){
        if (e instanceof RuntimeException){
            return (RuntimeException)e;
        } else {
            return new RuntimeException(e);
        }
    }

    /**
     * Determines if a zip archive contains a particular directory.
     *
     * @param zipfile path to the zipped file
     * @param target directory being looked for in the zip.
     * @return boolean whether or not the directory exists in the zip.
     */
    public static boolean zipDoesContainDir(String zipfile, String target) throws IOException {
        List<ZipEntry> entries = (List<ZipEntry>)Collections.list(new ZipFile(zipfile).entries());

        String targetDir = target + "/";
        for(ZipEntry entry : entries) {
            String name = entry.getName();
            if(name.startsWith(targetDir)) {
                return true;
            }
        }

        return false;
    }

    /**
     * Joins any number of maps together into a single map, combining their values into
     * a list, maintaining values in the order the maps were passed in. Nulls are inserted
     * for given keys when the map does not contain that key.
     *
     * i.e. joinMaps({'a' => 1, 'b' => 2}, {'b' => 3}, {'a' => 4, 'c' => 5}) ->
     *      {'a' => [1, null, 4], 'b' => [2, 3, null], 'c' => [null, null, 5]}
     *
     * @param maps variable number of maps to join - order affects order of values in output.
     * @return combined map
     */
    public static <K, V> Map<K, List<V>> joinMaps(Map<K, V>... maps) {
        Map<K, List<V>> ret = new HashMap<>();

        Set<K> keys = new HashSet<>();

        for(Map<K, V> map : maps) {
            keys.addAll(map.keySet());
        }

        for(Map<K, V> m : maps) {
            for(K key : keys) {
                V value = m.get(key);

                if(!ret.containsKey(key)) {
                    ret.put(key, new ArrayList<V>());
                }

                List<V> targetList = ret.get(key);
                targetList.add(value);
            }
        }
        return ret;
    }

    /**
     * Fills up chunks out of a collection (given a maximum amount of chunks)
     *
     * i.e. partitionFixed(5, [1,2,3]) -> [[1,2,3]]
     *      partitionFixed(5, [1..9]) -> [[1,2], [3,4], [5,6], [7,8], [9]]
     *      partitionFixed(3, [1..10]) -> [[1,2,3,4], [5,6,7], [8,9,10]]
     * @param maxNumChunks the maximum number of chunks to return
     * @param coll the collection to be chunked up
     * @return a list of the chunks, which are themselves lists.
     */
    public static <T> List<List<T>> partitionFixed(int maxNumChunks, Collection<T> coll) {
        List<List<T>> ret = new ArrayList<>();

        if(maxNumChunks == 0 || coll == null) {
            return ret;
        }

        Map<Integer, Integer> parts = integerDivided(coll.size(), maxNumChunks);

        // Keys sorted in descending order
        List<Integer> sortedKeys = new ArrayList<Integer>(parts.keySet());
        Collections.sort(sortedKeys, Collections.reverseOrder());


        Iterator<T> it = coll.iterator();
        for(Integer chunkSize : sortedKeys) {
            if(!it.hasNext()) { break; }
            Integer times = parts.get(chunkSize);
            for(int i = 0; i < times; i++) {
                if(!it.hasNext()) { break; }
                List<T> chunkList = new ArrayList<>();
                for(int j = 0; j < chunkSize; j++) {
                    if(!it.hasNext()) { break; }
                    chunkList.add(it.next());
                }
                ret.add(chunkList);
            }
        }

        return ret;
    }

    /**
     * Return a new instance of a pluggable specified in the conf.
     * @param conf The conf to read from.
     * @param configKey The key pointing to the pluggable class
     * @return an instance of the class or null if it is not specified.
     */
    public static Object getConfiguredClass(Map conf, Object configKey) {
        if (conf.containsKey(configKey)) {
            return newInstance((String)conf.get(configKey));
        }
        return null;
    }

    public static String logsFilename(String stormId, String port) {
        return stormId + FILE_PATH_SEPARATOR + port + FILE_PATH_SEPARATOR + "worker.log";
    }

    public static String eventLogsFilename(String stormId, String port) {
        return stormId + FILE_PATH_SEPARATOR + port + FILE_PATH_SEPARATOR + "events.log";
    }

    public static Object readYamlFile(String yamlFile) {
        try (FileReader reader = new FileReader(yamlFile)) {
            return new Yaml(new SafeConstructor()).load(reader);
        } catch(Exception ex) {
            LOG.error("Failed to read yaml file.", ex);
        }
        return null;
    }

    public static void setupDefaultUncaughtExceptionHandler() {
        Thread.setDefaultUncaughtExceptionHandler(new Thread.UncaughtExceptionHandler() {
                public void uncaughtException(Thread thread, Throwable thrown) {
                    try {
                        handleUncaughtException(thrown);
                    } catch (Error err) {
                        LOG.error("Received error in main thread.. terminating server...", err);
                        Runtime.getRuntime().exit(-2);
                    }
                }
            });
    }

    /**
     * Creates a new map with a string value in the map replaced with an
     * equivalently-lengthed string of '#'.
     * @param m The map that a value will be redacted from
     * @param key The key pointing to the value to be redacted
     * @return a new map with the value redacted. The original map will not be modified.
     */
    public static Map<Object, String> redactValue(Map<Object, String> m, Object key) {
        if(m.containsKey(key)) {
            HashMap<Object, String> newMap = new HashMap<>(m);
            String value = newMap.get(key);
            String redacted = new String(new char[value.length()]).replace("\0", "#");
            newMap.put(key, redacted);
            return newMap;
        }
        return m;
    }

    /**
     * Make sure a given key name is valid for the storm config.
     * Throw RuntimeException if the key isn't valid.
     * @param name The name of the config key to check.
     */
    private static final Set<String> disallowedKeys = new HashSet<>(Arrays.asList(new String[] {"/", ".", ":", "\\"}));
    public static void validateKeyName(String name) {

        for(String key : disallowedKeys) {
            if( name.contains(key) ) {
                throw new RuntimeException("Key name cannot contain any of the following: " + disallowedKeys.toString());
            }
        }
        if(name.trim().isEmpty()) {
            throw new RuntimeException("Key name cannot be blank");
        }
    }

    /**
     * Find the first item of coll for which pred.test(...) returns true.
     * @param pred The IPredicate to test for
     * @param coll The Collection of items to search through.
     * @return The first matching value in coll, or null if nothing matches.
     */
    public static <T> T findOne (IPredicate<T> pred, Collection<T> coll) {
        if(coll == null) {
            return null;
        }
        for(T elem : coll) {
            if (pred.test(elem)) {
                return elem;
            }
        }
        return null;
    }

    public static <T, U> T findOne (IPredicate<T> pred, Map<U, T> map) {
        if(map == null) {
            return null;
        }
        return findOne(pred, (Set<T>)map.entrySet());
    }

    public static String localHostname () throws UnknownHostException {
        return _instance.localHostnameImpl();
    }

    // Non-static impl methods exist for mocking purposes.
    protected String localHostnameImpl () throws UnknownHostException {
        return InetAddress.getLocalHost().getCanonicalHostName();
    }

    private static String memoizedLocalHostnameString = null;

    public static String memoizedLocalHostname () throws UnknownHostException {
        if (memoizedLocalHostnameString == null) {
            memoizedLocalHostnameString = localHostname();
        }
        return memoizedLocalHostnameString;
    }

    /**
     * Gets the storm.local.hostname value, or tries to figure out the local hostname
     * if it is not set in the config.
     * @param conf The storm config to read from
     * @return a string representation of the hostname.
    */
    public static String hostname (Map<String, Object> conf) throws UnknownHostException  {
        if (conf == null) {
            return memoizedLocalHostname();
        }
        Object hostnameString = conf.get(Config.STORM_LOCAL_HOSTNAME);
        if (hostnameString == null || hostnameString.equals("")) {
            return memoizedLocalHostname();
        }
        return (String)hostnameString;
    }

    public static String uuid() {
        return UUID.randomUUID().toString();
    }

    public static void exitProcess (int val, Object... msg) {
        StringBuilder errorMessage = new StringBuilder();
        errorMessage.append("Halting process: ");
        for (Object oneMessage: msg) {
            errorMessage.append(oneMessage);
        }
        String combinedErrorMessage = errorMessage.toString();
        LOG.error(combinedErrorMessage, new RuntimeException(combinedErrorMessage));
        Runtime.getRuntime().exit(val);
    }

    /**
     * "{:a 1 :b 1 :c 2} -> {1 [:a :b] 2 :c}"
     *
     * Example usage in java:
     *  Map<Integer, String> tasks;
     *  Map<String, List<Integer>> componentTasks = Utils.reverse_map(tasks);
     *
     * The order of he resulting list values depends on the ordering properties
     * of the Map passed in. The caller is responsible for passing an ordered
     * map if they expect the result to be consistently ordered as well.
     *
     * @param map to reverse
     * @return a reversed map
     */
    public static <K, V> HashMap<V, List<K>> reverseMap(Map<K, V> map) {
        HashMap<V, List<K>> rtn = new HashMap<V, List<K>>();
        if (map == null) {
            return rtn;
        }
        for (Entry<K, V> entry : map.entrySet()) {
            K key = entry.getKey();
            V val = entry.getValue();
            List<K> list = rtn.get(val);
            if (list == null) {
                list = new ArrayList<K>();
                rtn.put(entry.getValue(), list);
            }
            list.add(key);
        }
        return rtn;
    }

    /**
     * "[[:a 1] [:b 1] [:c 2]} -> {1 [:a :b] 2 :c}"
     * Reverses an assoc-list style Map like reverseMap(Map...)
     *
     * @param listSeq to reverse
     * @return a reversed map
     */
    public static HashMap reverseMap(List listSeq) {
        HashMap<Object, List<Object>> rtn = new HashMap();
        if (listSeq == null) {
            return rtn;
        }
        for (Object entry : listSeq) {
            List listEntry = (List) entry;
            Object key = listEntry.get(0);
            Object val = listEntry.get(1);
            List list = rtn.get(val);
            if (list == null) {
                list = new ArrayList<Object>();
                rtn.put(val, list);
            }
            list.add(key);
        }
        return rtn;
    }


    /**
     * @return the pid of this JVM, because Java doesn't provide a real way to do this.
     */
    public static String processPid() {
        String name = ManagementFactory.getRuntimeMXBean().getName();
        String[] split = name.split("@");
        if (split.length != 2) {
            throw new RuntimeException("Got unexpected process name: " + name);
        }
        return split[0];
    }

    public static int execCommand(String... command) throws ExecuteException, IOException {
        CommandLine cmd = new CommandLine(command[0]);
        for (int i = 1; i < command.length; i++) {
            cmd.addArgument(command[i]);
        }

        DefaultExecutor exec = new DefaultExecutor();
        return exec.execute(cmd);
    }

    /**
     * Extract dir from the jar to destdir
     *
     * @param jarpath Path to the jar file
     * @param dir Directory in the jar to pull out
     * @param destdir Path to the directory where the extracted directory will be put
     *
     */
    public static void extractDirFromJar(String jarpath, String dir, String destdir) {
        try (JarFile jarFile = new JarFile(jarpath)) {
            Enumeration<JarEntry> jarEnums = jarFile.entries();
            while (jarEnums.hasMoreElements()) {
                JarEntry entry = jarEnums.nextElement();
                if (!entry.isDirectory() && entry.getName().startsWith(dir)) {
                    File aFile = new File(destdir, entry.getName());
                    aFile.getParentFile().mkdirs();
                    try (FileOutputStream out = new FileOutputStream(aFile);
                         InputStream in = jarFile.getInputStream(entry)) {
                        IOUtils.copy(in, out);
                    }
                }
            }
        } catch (IOException e) {
            LOG.info("Could not extract {} from {}", dir, jarpath);
        }
    }

    public static void sendSignalToProcess(long lpid, int signum) throws IOException {
        String pid = Long.toString(lpid);
        try {
            if (isOnWindows()) {
                if (signum == SIGKILL) {
                    execCommand("taskkill", "/f", "/pid", pid);
                } else {
                    execCommand("taskkill", "/pid", pid);
                }
            } else {
                execCommand("kill", "-" + signum, pid);
            }
        } catch (ExecuteException e) {
            LOG.info("Error when trying to kill {}. Process is probably already dead.", pid);
        } catch (IOException e) {
            LOG.info("IOException Error when trying to kill {}.", pid);
            throw e;
        }
    }

    public static void forceKillProcess (String pid) throws IOException {
        sendSignalToProcess(Long.parseLong(pid), SIGKILL);
    }

    public static void killProcessWithSigTerm (String pid) throws IOException {
        sendSignalToProcess(Long.parseLong(pid), SIGTERM);
    }

    /**
     * Adds the user supplied function as a shutdown hook for cleanup.
     * Also adds a function that sleeps for a second and then halts the
     * runtime to avoid any zombie process in case cleanup function hangs.
     */
    public static void addShutdownHookWithForceKillIn1Sec (Runnable func) {
        Runnable sleepKill = new Runnable() {
            @Override
            public void run() {
                try {
                    Time.sleepSecs(1);
                    Runtime.getRuntime().halt(20);
                } catch (Exception e) {
                    LOG.warn("Exception in the ShutDownHook", e);
                }
            }
        };
        Runtime.getRuntime().addShutdownHook(new Thread(func));
        Runtime.getRuntime().addShutdownHook(new Thread(sleepKill));
    }

    /**
     * Returns the combined string, escaped for posix shell.
     * @param command the list of strings to be combined
     * @return the resulting command string
     */
    public static String shellCmd (List<String> command) {
        List<String> changedCommands = new ArrayList<>(command.size());
        for (String str: command) {
            if (str == null) {
                continue;
            }
            changedCommands.add("'" + str.replaceAll("'", "'\"'\"'") + "'");
        }
        return StringUtils.join(changedCommands, " ");
    }

    public static String scriptFilePath (String dir) {
        return dir + FILE_PATH_SEPARATOR + "storm-worker-script.sh";
    }

    public static String containerFilePath (String dir) {
        return dir + FILE_PATH_SEPARATOR + "launch_container.sh";
    }

    public static Object nullToZero (Object v) {
        return (v != null ? v : 0);
    }

    /**
     * Deletes a file or directory and its contents if it exists. Does not
     * complain if the input is null or does not exist.
     * @param path the path to the file or directory
     */
    public static void forceDelete(String path) throws IOException {
        _instance.forceDeleteImpl(path);
    }

    // Non-static impl methods exist for mocking purposes.
    protected void forceDeleteImpl(String path) throws IOException {
        LOG.debug("Deleting path {}", path);
        if (checkFileExists(path)) {
            try {
                FileUtils.forceDelete(new File(path));
            } catch (FileNotFoundException ignored) {}
        }
    }

    /**
     * Creates a symbolic link to the target
     * @param dir the parent directory of the link
     * @param targetDir the parent directory of the link's target
     * @param targetFilename the file name of the links target
     * @param filename the file name of the link
     * @throws IOException
     */
    public static void createSymlink(String dir, String targetDir,
            String targetFilename, String filename) throws IOException {
        Path path = Paths.get(dir, filename).toAbsolutePath();
        Path target = Paths.get(targetDir, targetFilename).toAbsolutePath();
        LOG.debug("Creating symlink [{}] to [{}]", path, target);
        if (!path.toFile().exists()) {
            Files.createSymbolicLink(path, target);
        }
    }

    /**
     * Convenience method for the case when the link's file name should be the
     * same as the file name of the target
     */
    public static void createSymlink(String dir, String targetDir,
                                     String targetFilename) throws IOException {
        Utils.createSymlink(dir, targetDir, targetFilename,
                            targetFilename);
    }

    /**
     * Returns a Collection of file names found under the given directory.
     * @param dir a directory
     * @return the Collection of file names
     */
    public static Collection<String> readDirContents(String dir) {
        Collection<String> ret = new HashSet<>();
        File[] files = new File(dir).listFiles();
        if (files != null) {
            for (File f: files) {
                ret.add(f.getName());
            }
        }
        return ret;
    }

    /**
     * Returns the value of java.class.path System property. Kept separate for
     * testing.
     * @return the classpath
     */
    public static String currentClasspath() {
        return _instance.currentClasspathImpl();
    }

    // Non-static impl methods exist for mocking purposes.
    public String currentClasspathImpl() {
        return System.getProperty("java.class.path");
    }

    /**
     * Returns a collection of jar file names found under the given directory.
     * @param dir the directory to search
     * @return the jar file names
     */
    private static List<String> getFullJars(String dir) {
        File[] files = new File(dir).listFiles(jarFilter);

        if(files == null) {
            return new ArrayList<>();
        }

        List<String> ret = new ArrayList<>(files.length);
        for (File f : files) {
            ret.add(Paths.get(dir, f.getName()).toString());
        }
        return ret;
    }
    private static final FilenameFilter jarFilter = new FilenameFilter() {
            @Override
            public boolean accept(File dir, String name) {
                return name.endsWith(".jar");
            }
        };


    public static String workerClasspath() {
        String stormDir = System.getProperty("storm.home");

        if (stormDir == null) {
            return Utils.currentClasspath();
        }

        String stormLibDir = Paths.get(stormDir, "lib").toString();
        String stormConfDir =
                System.getenv("STORM_CONF_DIR") != null ?
                System.getenv("STORM_CONF_DIR") :
                Paths.get(stormDir, "conf").toString();
        String stormExtlibDir = Paths.get(stormDir, "extlib").toString();
        String extcp = System.getenv("STORM_EXT_CLASSPATH");
        List<String> pathElements = new LinkedList<>();
        pathElements.addAll(Utils.getFullJars(stormLibDir));
        pathElements.addAll(Utils.getFullJars(stormExtlibDir));
        pathElements.add(extcp);
        pathElements.add(stormConfDir);

        return StringUtils.join(pathElements,
                CLASS_PATH_SEPARATOR);
    }

    public static String addToClasspath(String classpath,
                Collection<String> paths) {
        return _instance.addToClasspathImpl(classpath, paths);
    }

    // Non-static impl methods exist for mocking purposes.
    public String addToClasspathImpl(String classpath,
                Collection<String> paths) {
        if (paths == null || paths.isEmpty()) {
            return classpath;
        }
        List<String> l = new LinkedList<>();
        l.add(classpath);
        l.addAll(paths);
        return StringUtils.join(l, CLASS_PATH_SEPARATOR);
    }

    public static class UptimeComputer {
        int startTime = 0;

        public UptimeComputer() {
            startTime = Time.currentTimeSecs();
        }

        public int upTime() {
            return Time.deltaSecs(startTime);
        }
    }

    public static UptimeComputer makeUptimeComputer() {
        return _instance.makeUptimeComputerImpl();
    }

    // Non-static impl methods exist for mocking purposes.
    public UptimeComputer makeUptimeComputerImpl() {
        return new UptimeComputer();
    }

    /**
     * Writes a posix shell script file to be executed in its own process.
     * @param dir the directory under which the script is to be written
     * @param command the command the script is to execute
     * @param environment optional environment variables to set before running the script's command. May be  null.
     * @return the path to the script that has been written
     */
    public static String writeScript(String dir, List<String> command,
                                     Map<String,String> environment) throws IOException {
        String path = Utils.scriptFilePath(dir);
        try(BufferedWriter out = new BufferedWriter(new FileWriter(path))) {
            out.write("#!/bin/bash");
            out.newLine();
            if (environment != null) {
                for (String k : environment.keySet()) {
                    String v = environment.get(k);
                    if (v == null) {
                        v = "";
                    }
                    out.write(Utils.shellCmd(
                            Arrays.asList(
                                    "export",k+"="+v)));
                    out.write(";");
                    out.newLine();
                }
            }
            out.newLine();
            out.write("exec "+Utils.shellCmd(command)+";");
        }
        return path;
    }

    /**
     * A thread that can answer if it is sleeping in the case of simulated time.
     * This class is not useful when simulated time is not being used.
     */
    public static class SmartThread extends Thread {
        public boolean isSleeping() {
            return Time.isThreadWaiting(this);
        }
        public SmartThread(Runnable r) {
            super(r);
        }
    }

    /**
     * Creates a thread that calls the given code repeatedly, sleeping for an
     * interval of seconds equal to the return value of the previous call.
     *
     * The given afn may be a callable that returns the number of seconds to
     * sleep, or it may be a Callable that returns another Callable that in turn
     * returns the number of seconds to sleep. In the latter case isFactory.
     *
     * @param afn the code to call on each iteration
     * @param isDaemon whether the new thread should be a daemon thread
     * @param eh code to call when afn throws an exception
     * @param priority the new thread's priority
     * @param isFactory whether afn returns a callable instead of sleep seconds
     * @param startImmediately whether to start the thread before returning
     * @param threadName a suffix to be appended to the thread name
     * @return the newly created thread
     * @see java.lang.Thread
     */
    public static SmartThread asyncLoop(final Callable afn,
            boolean isDaemon, final Thread.UncaughtExceptionHandler eh,
            int priority, final boolean isFactory, boolean startImmediately,
            String threadName) {
        SmartThread thread = new SmartThread(new Runnable() {
            public void run() {
                Object s;
                try {
                    Callable fn = isFactory ? (Callable) afn.call() : afn;
                    while ((s = fn.call()) instanceof Long) {
                        Time.sleepSecs((Long) s);
                    }
                } catch (Throwable t) {
                    if (Utils.exceptionCauseIsInstanceOf(
                            InterruptedException.class, t)) {
                        LOG.info("Async loop interrupted!");
                        return;
                    }
                    LOG.error("Async loop died!", t);
                    throw new RuntimeException(t);
                }
            }
        });
        if (eh != null) {
            thread.setUncaughtExceptionHandler(eh);
        } else {
            thread.setUncaughtExceptionHandler(new Thread.UncaughtExceptionHandler() {
                public void uncaughtException(Thread t, Throwable e) {
                    LOG.error("Async loop died!", e);
                    Utils.exitProcess(1, "Async loop died!");
                }
            });
        }
        thread.setDaemon(isDaemon);
        thread.setPriority(priority);
        if (threadName != null && !threadName.isEmpty()) {
            thread.setName(thread.getName() +"-"+ threadName);
        }
        if (startImmediately) {
            thread.start();
        }
        return thread;
    }

    /**
     * Convenience method used when only the function and name suffix are given.
     * @param afn the code to call on each iteration
     * @param threadName a suffix to be appended to the thread name
     * @return the newly created thread
     * @see java.lang.Thread
     */
    public static SmartThread asyncLoop(final Callable afn, String threadName, final Thread.UncaughtExceptionHandler eh) {
        return asyncLoop(afn, false, eh, Thread.NORM_PRIORITY, false, true,
                threadName);
    }

    /**
     * Convenience method used when only the function is given.
     * @param afn the code to call on each iteration
     * @return the newly created thread
     */
    public static SmartThread asyncLoop(final Callable afn) {
        return asyncLoop(afn, false, null, Thread.NORM_PRIORITY, false, true,
                null);
    }

    /**
     * A callback that can accept an integer.
     * @param <V> the result type of method <code>call</code>
     */
    public interface ExitCodeCallable<V> extends Callable<V> {
        V call(int exitCode);
    }

    /**
     * Launch a new process as per {@link java.lang.ProcessBuilder} with a given
     * callback.
     * @param command the command to be executed in the new process
     * @param environment the environment to be applied to the process. Can be
     *                    null.
     * @param logPrefix a prefix for log entries from the output of the process.
     *                  Can be null.
     * @param exitCodeCallback code to be called passing the exit code value
     *                         when the process completes
     * @param dir the working directory of the new process
     * @return the new process
     * @throws IOException
     * @see java.lang.ProcessBuilder
     */
    public static Process launchProcess(List<String> command,
                                        Map<String,String> environment,
                                        final String logPrefix,
                                        final ExitCodeCallable exitCodeCallback,
                                        File dir)
            throws IOException {
        return _instance.launchProcessImpl(command, environment, logPrefix,
                exitCodeCallback, dir);
    }

    public Process launchProcessImpl(
            List<String> command,
            Map<String,String> cmdEnv,
            final String logPrefix,
            final ExitCodeCallable exitCodeCallback,
            File dir)
            throws IOException {
        ProcessBuilder builder = new ProcessBuilder(command);
        Map<String,String> procEnv = builder.environment();
        if (dir != null) {
            builder.directory(dir);
        }
        builder.redirectErrorStream(true);
        if (cmdEnv != null) {
            procEnv.putAll(cmdEnv);
        }
        final Process process = builder.start();
        if (logPrefix != null || exitCodeCallback != null) {
            Utils.asyncLoop(new Callable() {
                public Object call() {
                    if (logPrefix != null ) {
                        Utils.readAndLogStream(logPrefix,
                                process.getInputStream());
                    }
                    if (exitCodeCallback != null) {
                        try {
                            process.waitFor();
                        } catch (InterruptedException ie) {
                            LOG.info("{} interrupted", logPrefix);
                            exitCodeCallback.call(process.exitValue());
                        }
                    }
                    return null; // Run only once.
                }
            });
        }
        return process;
    }
}
