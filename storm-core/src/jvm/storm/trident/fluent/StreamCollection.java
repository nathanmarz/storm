package storm.trident.fluent;

import java.util.Map;
import java.util.HashMap;

import storm.trident.Stream;

public class StreamCollection {

    protected Map<String,Stream> streams;    

    public StreamCollection() {
        streams = new HashMap<String,Stream>();
    }

    public void addStream(String name, Stream s) {
        streams.put(name,s);
    }
    
    public Stream getStream(String name) {
        return streams.get(name);
    }

    public boolean hasStream(String name) {
        return streams.containsKey(name);
    }
}
