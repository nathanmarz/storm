package backtype.storm.utils;

import backtype.storm.generated.ComponentObject;
import clojure.lang.IFn;
import clojure.lang.RT;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.InputStreamReader;
import java.io.InputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import org.apache.thrift.TException;
import org.jvyaml.YAML;

public class Utils {
    public static final int DEFAULT_STREAM_ID = 1;

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

    public static Map readYamlConfig(String path) {
        try {
            Map ret = (Map) YAML.load(new FileReader(path));
            if(ret==null) ret = new HashMap();
            return new HashMap(ret);
        } catch (FileNotFoundException ex) {
            throw new RuntimeException(ex);
        }
    }

    public static Map findAndReadConfigFile(String name, boolean mustExist) {
        // It's important to use Utils.class here instead of Object.class here.
        // If Object.class is used, sbt can't find defaults.yaml
        InputStream is = Utils.class.getResourceAsStream("/" + name);
        if(is==null) {
            if(mustExist) throw new RuntimeException("Could not find config file on classpath " + name);
            else return new HashMap();
        }
        Map ret = (Map) YAML.load(new InputStreamReader(is));
        if(ret==null) ret = new HashMap();
        return new HashMap(ret);
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
        FileOutputStream out = new FileOutputStream(localFile);
        while(true) {
            byte[] chunk = toByteArray(client.getClient().downloadChunk(id));
            if(chunk.length==0) {
                break;
            }
            out.write(chunk);
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
    
    public static byte[] toByteArray(ByteBuffer bb){
		byte[] bytes = new byte[bb.capacity()];
		bb.get(bytes);
		return bytes;
	}
}
