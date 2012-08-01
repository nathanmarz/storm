package storm.trident.fluent;

public class UniqueIdGen {
    int _streamCounter = 0;
    
    public String getUniqueStreamId() {
        _streamCounter++;
        return "s" + _streamCounter;
    }

    int _stateCounter = 0;
    
    public String getUniqueStateId() {
        _stateCounter++;
        return "state" + _stateCounter;
    }    
}
