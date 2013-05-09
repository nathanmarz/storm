package storm.kafka.trident;

import java.io.Serializable;

public interface IBatchCoordinator extends Serializable {
    boolean isReady(long txid);
    void close();
}
