package backtype.storm.utils;

import backtype.storm.generated.ComponentCommon;
import backtype.storm.generated.ComponentObject;
import backtype.storm.generated.StormTopology;
import clojure.lang.IFn;
import clojure.lang.RT;
import org.apache.commons.io.FileUtils;
import org.apache.thrift7.TException;
import org.yaml.snakeyaml.Yaml;

import java.io.*;
import java.net.URL;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.WritableByteChannel;
import java.util.*;
import java.util.jar.JarEntry;
import java.util.jar.JarInputStream;

public class Utils {
    public static final String DEFAULT_STREAM_ID = "default";

    public static byte[] serialize(Object obj) {
        try {
            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            ObjectOutputStream oos = new ObjectOutputStream(bos);
            oos.writeObject(obj);
            oos.close();
            return bos.toByteArray();
        } catch(IOException ioe) {
            throw new RuntimeException(ioe);
        }
    }

    public static Object deserialize(byte[] serialized) {
        try {
            ByteArrayInputStream bis = new ByteArrayInputStream(serialized);
            ObjectInputStream ois = new ObjectInputStream(bis);
            Object ret = ois.readObject();
            ois.close();
            return ret;
        } catch(IOException ioe) {
            throw new RuntimeException(ioe);
        } catch(ClassNotFoundException e) {
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

    public static Map findAndReadConfigFile(String name, boolean mustExist) {
        try {
            Enumeration resources = Thread.currentThread().getContextClassLoader().getResources(name);
            if(!resources.hasMoreElements()) {
                if(mustExist) throw new RuntimeException("Could not find config file on classpath " + name);
                else return new HashMap();
            }
            URL resource = (URL) resources.nextElement();
            Yaml yaml = new Yaml();
            Map ret = (Map) yaml.load(new InputStreamReader(resource.openStream()));
            if(ret==null) ret = new HashMap();
            
            if(resources.hasMoreElements()) {
                throw new RuntimeException("Found multiple " + name + " resources");
            }
            return new HashMap(ret);
            
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static Map findAndReadConfigFile(String name) {
       return findAndReadConfigFile(name, true);
    }

    public static Map readDefaultConfig() {
        return findAndReadConfigFile("defaults.yaml", true);
    }

    public static Map readStormConfig() {
        Map ret = readDefaultConfig();
        Map storm = findAndReadConfigFile("storm.yaml", false);
        ret.putAll(storm);
        return ret;
    }

    public static Object getSetComponentObject(ComponentObject obj) {
        if(obj.getSetField()==ComponentObject._Fields.SERIALIZED_JAVA) {
            return Utils.deserialize(obj.get_serialized_java());
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

    public static void downloadFromMaster(Map conf, String file, String localFile) throws IOException, TException {
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
    
    public static Integer toInteger(Object o) {
        if(o==null) return null;
        else if(o instanceof Integer) return (Integer) o;
        else if(o instanceof Long) return ((Long) o).intValue();
        else throw new RuntimeException("Could not coerce " + o + " to integer");
    }
    
    public static IFn loadClojureFn(String namespace, String name) {
        try {
          clojure.lang.Compiler.eval(RT.readString("(require '" + namespace + ")"));
        } catch (Exception e) {
          //if playing from the repl and defining functions, file won't exist
        }
        return (IFn) RT.var(namespace, name).deref();        
    }
    
    public static boolean isSystemStream(String id) {
        return id.startsWith("__");
    }
    
    public static boolean isSystemComponent(String id) {
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
        if(o instanceof Long) {
            return ((Long) o ).intValue();
        } else if (o instanceof Integer) {
            return (Integer) o;
        } else if (o instanceof Short) {
            return ((Short) o).intValue();
        } else {
            throw new IllegalArgumentException("Don't know how to convert " + o + " + to int");
        }
    }
    
    public static void copyResourcesFromJar(File jar, String resources, File destination) {
        JarInputStream jarInputStream = null;

        try {
            jarInputStream = new JarInputStream(new FileInputStream(jar));
            JarEntry jarEntry = null;
            while ((jarEntry = jarInputStream.getNextJarEntry()) != null) {
                if (jarEntry.isDirectory()) {
                    continue;
                }
                if (jarEntry.getName().startsWith(resources)) {
                    int size = (int) jarEntry.getSize();
                    byte[] b = new byte[(int) size];
                    int rb = 0;
                    int chunk = 0;
                    while ((size - rb) > 0) {
                        chunk = jarInputStream.read(b, rb, size - rb);
                        if (chunk == -1) {
                            break;
                        }
                        rb += chunk;
                    }

                    String fileName = jarEntry.getName();
                    String toFileName = destination.getPath() + File.separator + fileName;

                    FileOutputStream fileOutputStream = FileUtils.openOutputStream(new File(toFileName));
                    fileOutputStream.write(b);
                    fileOutputStream.close();
                }
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        } finally {
            if (jarInputStream != null) {
                try {
                    jarInputStream.close();
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
        }
    }
}
