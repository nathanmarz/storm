package storm.dedup;

import java.util.HashSet;
import java.util.Set;

import storm.dedup.impl.DedupBoltExecutor;
import storm.dedup.impl.DedupSpoutExecutor;

import backtype.storm.generated.StormTopology;
import backtype.storm.topology.InputDeclarer;
import backtype.storm.topology.TopologyBuilder;

/**
 * TopologyBuilder for IDedupSpout and IDedupBolt.
 * 
 * It automatically set bolt subscribe each spout's dedup stream.
 *
 */
public class DedupTopologyBuilder {
  private TopologyBuilder builder;
  
  private Set<String> spouts;
  private Set<String> bolts;
  
  public DedupTopologyBuilder() {
    this.builder = new TopologyBuilder();
    this.spouts = new HashSet<String>();
    this.bolts = new HashSet<String>();
  }

  public StormTopology createTopology() {
    return builder.createTopology();
  }
  
  public void setSpout(String id, 
      IDedupSpout spout, Integer parallelism_hint) {
    if (!spouts.add(id)) {
      throw new RuntimeException("spout already exist " + id);
    }
    builder.setSpout(id, 
        new DedupSpoutExecutor(spout), parallelism_hint);
  }
  
  public InputDeclarer setBolt(String id, 
      IDedupBolt bolt, Integer parallelism_hint) {
    if (!bolts.add(id)) {
      throw new RuntimeException("bolt already exist " + id);
    }
    InputDeclarer inputDeclarer = builder.setBolt(id, 
        new DedupBoltExecutor(bolt), parallelism_hint);
    
    // automatically set bolt subscribe each spout's dedup stream
    for (String spoutID : spouts) {
      inputDeclarer.allGrouping(spoutID, DedupConstants.DEDUP_STREAM_ID);
    }
    return inputDeclarer;
  }
}
