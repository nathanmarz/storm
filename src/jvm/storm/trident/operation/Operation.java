package storm.trident.operation;

import java.io.Serializable;
import java.util.Map;

public interface Operation extends Serializable {
    void prepare(Map conf, TridentOperationContext context);
    void cleanup();
}
