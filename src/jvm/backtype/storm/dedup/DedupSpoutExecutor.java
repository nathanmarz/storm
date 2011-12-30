package backtype.storm.dedup;

import java.io.IOException;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.OutputFieldsDeclarer;

public class DedupSpoutExecutor implements IRichSpout {
  
  private static final Log LOG = LogFactory.getLog(DedupSpoutExecutor.class);

  private DedupSpoutContext context;
  
  private IDedupSpout spout;
  
  public DedupSpoutExecutor(IDedupSpout spout) {
    this.spout = spout;
  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer declarer) {
    context.setOutputFieldsDeclarer(declarer);
    spout.declareOutputFields(context);
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
}
