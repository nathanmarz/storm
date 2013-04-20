package storm.trident.state;

/**
 * There's 3 different kinds of state:
 * 
 * 1. non-transactional: ignores commits, updates are permanent. no rollback. a cassandra incrementing state would be like this
 * 2. repeat-transactional: idempotent as long as all batches for a txid are identical
 * 3. opaque-transactional: the most general kind of state. updates are always done
 *          based on the previous version of the value if the current commit = latest stored commit
 *          Idempotent even if the batch for a txid can change.
 * 
 * repeat transactional is idempotent for transactional spouts
 * opaque transactional is idempotent for opaque or transactional spouts
 * 
 * Trident should log warnings when state is idempotent but updates will not be idempotent
 * because of spout
 */
// retrieving is encapsulated in Retrieval interface
public interface State {
    void beginCommit(Long txid); // can be null for things like partitionPersist occuring off a DRPC stream
    void commit(Long txid);
}
