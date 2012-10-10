package storm.trident.state;

import java.util.Iterator;
import java.util.List;

/* Container of a collection of tuples */
public interface ITupleCollection {
    public Iterator<List<Object>> getTuples();
}
