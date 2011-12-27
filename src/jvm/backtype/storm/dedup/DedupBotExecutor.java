package backtype.storm.dedup;

import java.io.IOException;
import java.util.Map;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;

public class DedupBotExecutor implements IRichBolt {

  private DedupBoltContext context;
  
  private IDedupBolt bolt;
  
  public DedupBotExecutor(IDedupBolt bolt) {
    this.bolt = bolt;
  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer declarer) {
    context.setOutputFieldsDeclarer(declarer);
    bolt.declareOutputFields(context);
  }

  @Override
  public void prepare(Map stormConf, TopologyContext context,
      OutputCollector collector) {
    try {
      this.context = new DedupBoltContext(stormConf, context, collector);
    } catch (IOException e) {
      System.exit(1);
    }
    bolt.prepare(this.context);
  }
  
  @Override
  public void cleanup() {
    bolt.cleanup(context);
  }

  @Override
  public void execute(Tuple input) {
    try {
      context.execute(bolt, input);
    } catch (IOException e) {
      System.exit(2);
    }
  }

}
