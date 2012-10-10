package storm.trident.spout;


public interface IBatchID {
    Object getId();
    int getAttemptId();
}
