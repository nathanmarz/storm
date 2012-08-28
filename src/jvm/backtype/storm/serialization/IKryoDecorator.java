package backtype.storm.serialization;
import com.esotericsoftware.kryo.Kryo;

public interface IKryoDecorator {
    void decorate(Kryo k);
}
