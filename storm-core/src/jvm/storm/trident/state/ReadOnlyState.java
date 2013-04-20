package storm.trident.state;

public class ReadOnlyState implements State {

    @Override
    public void beginCommit(Long txid) {
        throw new UnsupportedOperationException("This state is read-only and does not support updates");
    }

    @Override
    public void commit(Long txid) {
        throw new UnsupportedOperationException("This state is read-only and does not support updates");
    }    
}
