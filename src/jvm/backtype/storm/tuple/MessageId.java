package backtype.storm.tuple;

import backtype.storm.utils.Utils;
import backtype.storm.utils.WritableUtils;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

public class MessageId {
    private Map<Long, Long> _anchorsToIds;
    
    public static long generateId() {
        return Utils.randomLong();
    }

    public static MessageId makeUnanchored() {
        return makeId(new HashMap<Long, Long>());
    }
        
    public static MessageId makeId(Map<Long, Long> anchorsToIds) {
        return new MessageId(anchorsToIds);
    }
        
    public static MessageId makeRootId(long id, long val) {
        Map<Long, Long> anchorsToIds = new HashMap<Long, Long>();
        anchorsToIds.put(id, val);
        return new MessageId(anchorsToIds);
    }
    
    protected MessageId(Map<Long, Long> anchorsToIds) {
        _anchorsToIds = anchorsToIds;
    }

    public Map<Long, Long> getAnchorsToIds() {
        return _anchorsToIds;
    }

    public Set<Long> getAnchors() {
        return _anchorsToIds.keySet();
    }    
    
    @Override
    public int hashCode() {
        return _anchorsToIds.hashCode();
    }

    @Override
    public boolean equals(Object other) {
        if(other instanceof MessageId) {
            return _anchorsToIds.equals(((MessageId) other)._anchorsToIds);
        } else {
            return false;
        }
    }

    @Override
    public String toString() {
        return _anchorsToIds.toString();
    }

    public void serialize(DataOutputStream out) throws IOException {
        WritableUtils.writeVInt(out, _anchorsToIds.size());
        for(Entry<Long, Long> anchorToId: _anchorsToIds.entrySet()) {
            out.writeLong(anchorToId.getKey());
            out.writeLong(anchorToId.getValue());
        }
    }

    public static MessageId deserialize(DataInputStream in) throws IOException {
        int numAnchors = WritableUtils.readVInt(in);
        Map<Long, Long> anchorsToIds = new HashMap<Long, Long>();
        for(int i=0; i<numAnchors; i++) {
            anchorsToIds.put(in.readLong(), in.readLong());
        }
        return new MessageId(anchorsToIds);
    }
}