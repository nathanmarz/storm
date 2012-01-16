package storm.dedup.example;

import java.io.Serializable;

import storm.dedup.Bytes;
import storm.dedup.DedupConstants;
import storm.dedup.DedupTopologyBuilder;
import storm.dedup.IDedupBolt;
import storm.dedup.IDedupContext;
import storm.dedup.IDedupSpout;

import backtype.storm.Config;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class WordTopTopology {
  
  /**
   * GenerateSpout
   *
   */
  public static class GenerateSpout implements IDedupSpout, Serializable {

    @Override
    public void ack(String messageId) {
      // nothing
    }

    @Override
    public void close(IDedupContext context) {
      // nothing
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
      declarer.declare(new Fields("LINE"));
    }

    @Override
    public void fail(String messageId) {
      // nothing
    }

    @Override
    public void nextTuple(IDedupContext context) {
      // TODO real tuple
      context.emit(new Values("this is the text line"));
      try {
        Thread.sleep(1000);
      } catch (InterruptedException e) {
        // nothing
      }
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
  public static class SplitBolt implements IDedupBolt, Serializable {

    @Override
    public void cleanup(IDedupContext context) {
      // nothing
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
      declarer.declare(new Fields("WORD"));
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
  public static class CountBolt implements IDedupBolt, Serializable {

    @Override
    public void cleanup(IDedupContext context) {
      // nothing
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
      declarer.declare(new Fields("WORD", "COUNT"));
    }

    @Override
    public void execute(IDedupContext context, Tuple input) {
      String word = input.getStringByField("WORD");
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
   * TopBolt
   */
  public static class TopBolt implements IDedupBolt, Serializable {

    @Override
    public void cleanup(IDedupContext context) {
      // nothing
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
      // nothing
    }

    @Override
    public void execute(IDedupContext context, Tuple input) {
      String word = input.getStringByField("WORD");
      Long count = input.getLongByField("COUNT");
      context.setState(Bytes.toBytes(word), Bytes.toBytes(count.toString()));
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
    
    builder.setSpout("generate-spout", new GenerateSpout(), 2);
    
    builder.setBolt("split-bolt", new SplitBolt(), 4)
      .shuffleGrouping("generate-spout");
    
    builder.setBolt("count-bolt", new CountBolt(), 8)
      .fieldsGrouping("split-bolt", new Fields("WORD"));
    
    builder.setBolt("top-bolt", new TopBolt(), 1)
    .fieldsGrouping("count-bolt", new Fields("WORD"));
    
    Config conf = new Config();
    conf.setDebug(true);
    
    if (args != null && args.length > 3) {
      conf.setNumWorkers(Integer.parseInt(args[1]));
      conf.put(DedupConstants.STATE_STORE_NAME, args[2]);
      StormSubmitter.submitTopology(args[0], conf, builder.createTopology());
    }
  }
}
