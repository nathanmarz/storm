package backtype.storm.transactional;

public interface ITransactional {
    void startTransaction(long txid, String spoutId);
    void commitTransaction(long txid);
}
