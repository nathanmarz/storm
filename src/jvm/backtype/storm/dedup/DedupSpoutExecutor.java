package backtype.storm.dedup;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;

public class DedupSpoutExecutor implements IRichSpout, OutputFieldsDeclarer {
  
  private static final Log LOG = LogFactory.getLog(DedupSpoutExecutor.class);

  private OutputFieldsDeclarer declarer;
  
  private DedupSpoutContext context;
  
  private IDedupSpout spout;
  
  public DedupSpoutExecutor(IDedupSpout spout) {
    this.spout = spout;
  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer declarer) {
    this.declarer = declarer;
    spout.declareOutputFields(this);
  }

  @Override
  public void open(Map conf, TopologyContext context,
      SpoutOutputCollector collector) {
    // create DedupSpoutContext
    try {
      this.context = new DedupSpoutContext(conf, context, collector);
    } catch (IOException e) {
      LOG.warn("create DedupSpoutContext error", e);
      throw new RuntimeException(e);
    }
    // open user spout and offer DedupSpoutContext
    spout.open(this.context);
  }
  
  @Override
  public boolean isDistributed() {
    return true;
  }

  @Override
  public void close() {
    spout.close(context);
  }

  @Override
  public void nextTuple() {
    try {
      context.nextTuple(spout);
    } catch (IOException e) {
      LOG.warn("generate tuple error", e);
      throw new RuntimeException(e);
    }
  }

  @Override
  public void ack(Object msgId) {
    try {
      context.ack(msgId);
    } catch (IOException e) {
      LOG.warn("ack tuple error " + msgId, e);
      throw new RuntimeException(e);
    }
  }

  @Override
  public void fail(Object msgId) {
    try {
      context.fail(msgId);
    } catch (IOException e) {
      LOG.warn("fail tuple error " + msgId, e);
      throw new RuntimeException(e);
    }
  }

  /**
   * impl OutputFieldsDeclarer
   */
  
  @Override
  public void declare(Fields fields) {
    // add tow fields to original fields
    List<String> fieldList = fields.toList();
    fieldList.add(DedupConstants.TUPLE_ID_FIELD);
    fieldList.add(DedupConstants.TUPLE_TYPE_FIELD);
    declarer.declare(new Fields(fieldList));
    // declare DEDUP_STREAM with to fields
    declarer.declareStream(DedupConstants.DEDUP_STREAM_ID, 
        new Fields(DedupConstants.TUPLE_ID_FIELD, 
            DedupConstants.TUPLE_TYPE_FIELD));
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
