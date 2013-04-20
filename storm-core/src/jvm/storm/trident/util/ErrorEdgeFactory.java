package storm.trident.util;

import java.io.Serializable;
import org.jgrapht.EdgeFactory;

public class ErrorEdgeFactory implements EdgeFactory, Serializable {
    @Override
    public Object createEdge(Object v, Object v1) {
        throw new RuntimeException("Edges should be made explicitly");
    }        
}
