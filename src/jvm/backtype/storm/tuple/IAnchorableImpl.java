package backtype.storm.tuple;

public interface IAnchorableImpl extends IAnchorable {
    Tuple getUnderlyingTuple();
}
