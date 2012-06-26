package backtype.storm.transactional;

/**
 * This marks an IBatchbolth within a transactional topology as a committer. This causes the 
 * finishBatch method to be called in order of the transactions.
 */
public interface ICommitter {
    
}
