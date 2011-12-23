package backtype.storm.dedup;

import java.util.Map;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.OutputFieldsDeclarer;

public class DedupSpoutExecutor implements IRichSpout {

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
    this.context = new DedupSpoutContext(conf, context, collector);
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
    context.beforeNextTuple();
    spout.nextTuple(context);
    context.afterNextTuple();
  }

  @Override
  public void ack(Object msgId) {
    context.ack(msgId);
  }

  @Override
  public void fail(Object msgId) {
    context.fail(msgId);
  }
}
