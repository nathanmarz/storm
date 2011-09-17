package backtype.storm.topology;

import backtype.storm.tuple.Fields;


public interface OutputFieldsDeclarer {
    /**
     * Uses default stream id.
     */
    public void declare(Fields fields);
    public void declare(boolean direct, Fields fields);
    
    public void declareStream(int streamId, Fields fields);
    public void declareStream(int streamId, boolean direct, Fields fields);
}
