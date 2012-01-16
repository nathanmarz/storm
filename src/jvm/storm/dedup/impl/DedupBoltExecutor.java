package storm.dedup.impl;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import storm.dedup.DedupConstants;
import storm.dedup.IDedupBolt;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;

public class DedupBoltExecutor implements IRichBolt, OutputFieldsDeclarer {
  
  private static final Log LOG = LogFactory.getLog(DedupBoltExecutor.class);

  private transient OutputFieldsDeclarer declarer;
  
  private transient DedupBoltContext context;
  
  private IDedupBolt bolt;
  
  public DedupBoltExecutor(IDedupBolt bolt) {
    this.bolt = bolt;
  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer declarer) {
    this.declarer = declarer;
    bolt.declareOutputFields(this);
  }

  @Override
  public void prepare(Map stormConf, TopologyContext context,
      OutputCollector collector) {
    try {
      this.context = new DedupBoltContext(stormConf, context, collector);
    } catch (IOException e) {
      LOG.warn("create DedupBoltContext error", e);
      throw new RuntimeException(e);
    }
    bolt.prepare(this.context);
  }
  
  @Override
  public void cleanup() {
    bolt.cleanup(context);
    try {
      context.close();
    } catch (IOException e) {
      LOG.warn("close error", e);
      throw new RuntimeException(e);
    }
  }

  @Override
  public void execute(Tuple input) {
    try {
      context.execute(bolt, input);
    } catch (IOException e) {
      LOG.warn("process tuple error", e);
      throw new RuntimeException(e);
    }
  }

  /**
   * impl OutputFieldsDeclarer
   */
  
  @Override
  public void declare(Fields fields) {
    // add TUPLE_ID_FIELD to original fields
    List<String> fieldList = fields.toList();
    fieldList.add(DedupConstants.TUPLE_ID_FIELD);
    declarer.declare(new Fields(fieldList));
  }

  @Override
  public void declare(boolean direct, Fields fields) {
    // TODO Auto-generated method stub
    
  }

  @Override
  public void declareStream(String streamId, Fields fields) {
    // TODO Auto-generated method stub
    
  }

  @Override
  public void declareStream(String streamId, boolean direct, Fields fields) {
    // TODO Auto-generated method stub
    
  }

}
