package backtype.storm.state;

import java.util.List;

/* Container of a collection of tuples */
public interface ITupleCollection {
    public List<List<Object>> getTuples();
}
