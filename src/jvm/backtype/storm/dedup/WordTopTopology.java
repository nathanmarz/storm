package backtype.storm.dedup;

import backtype.storm.Config;
import backtype.storm.StormSubmitter;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class WordTopTopology {
  
  /**
   * GenerateSpout
   *
   */
  public static class GenerateSpout implements IDedupSpout {

    @Override
    public void ack(long messageId) {
      // nothing
    }

    @Override
    public void close(IDedupContext context) {
      // nothing
    }

    @Override
    public void declareOutputFields(IDedupContext context) {
      context.declare(new Fields("LINE"));
    }

    @Override
    public void fail(long messageId) {
      // nothing
    }

    @Override
    public void nextTuple(IDedupContext context) {
      // TODO real tuple
      context.emit(new Values("this is the text line"));
    }

    @Override
    public void open(IDedupContext context) {
      // nothing
    }
    
  }
  
  /**
   * 
   * SplitBolt
   *
   */
  public static class SplitBolt implements IDedupBolt {

    @Override
    public void cleanup(IDedupContext context) {
      // nothing
    }

    @Override
    public void declareOutputFields(IDedupContext context) {
      context.declare(new Fields("WORD"));
    }

    @Override
    public void execute(IDedupContext context, Tuple input) {
      String line = input.getString(0);
      String[] words = line.split(" ");
      if (words != null) {
        for (String word : words) {
          context.emit(new Values(word));
        }
      }
    }

    @Override
    public void prepare(IDedupContext context) {
      // nothing
    }
    
  }
  
  /**
   * CountBolt
   */
  public static class CountBolt implements IDedupBolt {

    @Override
    public void cleanup(IDedupContext context) {
      // nothing
    }

    @Override
    public void declareOutputFields(IDedupContext context) {
      context.declare(new Fields("WORD", "COUNT"));
    }

    @Override
    public void execute(IDedupContext context, Tuple input) {
      String word = input.getString(0);
      long count = 0;
      byte[] bytes = context.getState(Bytes.toBytes(word));
      if (bytes != null) {
        count = Long.parseLong(Bytes.toString(bytes));
      }
      count++;
      context.setState(Bytes.toBytes(word), 
          Bytes.toBytes(Long.toString(count)));
    }

    @Override
    public void prepare(IDedupContext context) {
      // nothing
    }
    
  }

  /**
   * @param args
   */
  public static void main(String[] args) throws Exception {
    DedupTopologyBuilder builder = new DedupTopologyBuilder();
    
    builder.setSpout("generate spout", new GenerateSpout(), 1);
    
    builder.setBolt("split bolt", new SplitBolt(), 4)
      .shuffleGrouping("generate spout")
      .directGrouping("generate spout", DedupConstants.DEDUP_STREAM_ID);
    
    builder.setBolt("count bolt", new CountBolt(), 8)
      .fieldsGrouping("split bolt", new Fields("WORD"))
      .directGrouping("generate spout", DedupConstants.DEDUP_STREAM_ID);
    
    Config conf = new Config();
    conf.setDebug(true);
    
    if (args != null && args.length > 2) {
      conf.setNumWorkers(Integer.parseInt(args[1]));
      StormSubmitter.submitTopology(args[0], conf, builder.createTopology());
    }
  }
}
