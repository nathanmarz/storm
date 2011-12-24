package backtype.storm.coordination;

import backtype.storm.tuple.IAnchorable;


public interface FinishedTuple extends IAnchorable {
    Object getId();
}
