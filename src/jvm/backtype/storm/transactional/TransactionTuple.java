package backtype.storm.transactional;

import backtype.storm.tuple.Tuple;


// should be an anchorable, not a tuple
public class TransactionTuple  {
    public TransactionTuple(Tuple delegate) {
        
    }
    
    // add convenience method for getting the transaction id
}
