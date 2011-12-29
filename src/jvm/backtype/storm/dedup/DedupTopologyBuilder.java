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
  
  public DedupTopologyBuilder() {
    this.builder = new TopologyBuilder();
  }

  public StormTopology createTopology() {
    return builder.createTopology();
  }
  
  public InputDeclarer setBolt(String id, 
      IDedupBolt bolt, Integer parallelism_hint) {
    return builder.setBolt(id, 
        new DedupBoltExecutor(bolt), parallelism_hint);
  }
  
  public void setSpout(String id, 
      IDedupSpout spout, Integer parallelism_hint) {
    builder.setSpout(id, 
        new DedupSpoutExecutor(spout), parallelism_hint);
  }
}
