package storm.trident.planner;

import backtype.storm.tuple.Fields;


public class SpoutNode extends Node {
    public static enum SpoutType {
        DRPC,
        BATCH
    }
    
    public Object spout;
    public String txId; //where state is stored in zookeeper (only for batch spout types)
    public SpoutType type;
    
    public SpoutNode(String streamId, Fields allOutputFields, String txid, Object spout, SpoutType type) {
        super(streamId, null, allOutputFields);
        this.txId = txid;
        this.spout = spout;
        this.type = type;
    }
}
