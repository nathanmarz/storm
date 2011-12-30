package backtype.storm.dedup;

import backtype.storm.generated.StormTopology;
import backtype.storm.topology.InputDeclarer;
import backtype.storm.topology.TopologyBuilder;

/**
 * TopologyBuilder for IDedupSpout and IDedupBolt
 *
 */
public class DedupTopologyBuilder {
  private TopologyBuilder builder;
  
  private String spoutID;
  
  public DedupTopologyBuilder() {
    this.builder = new TopologyBuilder();
  }

  public StormTopology createTopology() {
    return builder.createTopology();
  }
  
  public InputDeclarer setBolt(String id, 
      IDedupBolt bolt, Integer parallelism_hint) {
    InputDeclarer inputDeclarer = builder.setBolt(id, 
        new DedupBoltExecutor(bolt), parallelism_hint);
    inputDeclarer.allGrouping(spoutID, DedupConstants.DEDUP_STREAM_ID);
    
    return inputDeclarer;
  }
  
  public void setSpout(String id, 
      IDedupSpout spout, Integer parallelism_hint) {
    if (spoutID != null)
      throw new RuntimeException("spout alread set to " + spoutID);
    
    spoutID = id;
    builder.setSpout(id, 
        new DedupSpoutExecutor(spout), parallelism_hint);
  }
}
