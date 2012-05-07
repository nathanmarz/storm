package backtype.storm.utils;

import org.apache.commons.io.FileUtils;
import java.io.File;
import java.util.Map;
import java.util.HashMap;
import java.io.IOException;


/**
 * A simple, durable, atomic K/V database. *Very inefficient*, should only be used for occasional reads/writes.
 * Every read/write hits disk.
 */
public class LocalState {
    private VersionedStore _vs;
    
    public LocalState(String backingDir) throws IOException {
        _vs = new VersionedStore(backingDir);
    }
    
    public synchronized Map<Object, Object> snapshot() throws IOException {
        String latestPath = _vs.mostRecentVersionPath();
        if(latestPath==null) return new HashMap<Object, Object>();
        return (Map<Object, Object>) Utils.deserialize(FileUtils.readFileToByteArray(new File(latestPath)));
    }
    
    public Object get(Object key) throws IOException {
        return snapshot().get(key);
    }
    
    public synchronized void put(Object key, Object val) throws IOException {
        Map<Object, Object> curr = snapshot();
        curr.put(key, val);
        persist(curr);
    }

    public synchronized void remove(Object key) throws IOException {
        Map<Object, Object> curr = snapshot();
        curr.remove(key);
        persist(curr);
    }
    
    private void persist(Map<Object, Object> val) throws IOException {
        byte[] toWrite = Utils.serialize(val);
        String newPath = _vs.createVersion();
        FileUtils.writeByteArrayToFile(new File(newPath), toWrite);
        _vs.succeedVersion(newPath);
        _vs.cleanup(4);
    }
}