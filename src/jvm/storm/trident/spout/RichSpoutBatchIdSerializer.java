package storm.trident.spout;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;


public class RichSpoutBatchIdSerializer extends Serializer<RichSpoutBatchId> {

    @Override
    public void write(Kryo kryo, Output output, RichSpoutBatchId id) {
        output.writeLong(id._id);
    }

    @Override
    public RichSpoutBatchId read(Kryo kryo, Input input, Class type) {
        long l = input.readLong();
        return new RichSpoutBatchId(l);
    }    
}
