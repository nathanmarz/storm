package storm.trident.planner;

import backtype.storm.generated.Grouping;
import backtype.storm.tuple.Fields;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.List;
import storm.trident.util.TridentUtils;


public class PartitionNode extends Node {
    public transient Grouping thriftGrouping;
    
    //has the streamid/outputFields of the node it's doing the partitioning on
    public PartitionNode(String streamId, String name, Fields allOutputFields, Grouping grouping) {
        super(streamId, name, allOutputFields);
        this.thriftGrouping = grouping;
    }
    
    private void writeObject(ObjectOutputStream oos) throws IOException {
        oos.defaultWriteObject();
        byte[] ser = TridentUtils.thriftSerialize(thriftGrouping);
        oos.writeInt(ser.length);
        oos.write(ser);
    }

    private void readObject(ObjectInputStream ois) throws ClassNotFoundException, IOException {
        ois.defaultReadObject();
        byte[] ser = new byte[ois.readInt()];
        ois.readFully(ser);
        this.thriftGrouping = TridentUtils.thriftDeserialize(Grouping.class, ser);
    }
}
