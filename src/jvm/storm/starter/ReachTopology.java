package storm.starter;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.LocalDRPC;
import backtype.storm.StormSubmitter;
import backtype.storm.coordination.BatchOutputCollector;
import backtype.storm.coordination.CoordinatedBolt.FinishedCallback;
import backtype.storm.drpc.LinearDRPCTopologyBuilder;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.topology.base.BaseBatchBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * This is a good example of doing complex Distributed RPC on top of Storm. This 
 * program creates a topology that can compute the reach for any URL on Twitter
 * in realtime by parallelizing the whole computation. 
 * 
 * Reach is the number of unique people exposed to a URL on Twitter. To compute reach,
 * you have to get all the people who tweeted the URL, get all the followers of all those people,
 * unique that set of followers, and then count the unique set. It's an intense computation 
 * that can involve thousands of database calls and tens of millions of follower records.
 * 
 * This Storm topology does every piece of that computation in parallel, turning what would be a 
 * computation that takes minutes on a single machine into one that takes just a couple seconds.
 * 
 * For the purposes of demonstration, this topology replaces the use of actual DBs with
 * in-memory hashmaps. 
 * 
 * See https://github.com/nathanmarz/storm/wiki/Distributed-RPC for more information on Distributed RPC.
 */
public class ReachTopology {
    public static Map<String, List<String>> TWEETERS_DB = new HashMap<String, List<String>>() {{
       put("foo.com/blog/1", Arrays.asList("sally", "bob", "tim", "george", "nathan")); 
       put("engineering.twitter.com/blog/5", Arrays.asList("adam", "david", "sally", "nathan")); 
       put("tech.backtype.com/blog/123", Arrays.asList("tim", "mike", "john")); 
    }};
    
    public static Map<String, List<String>> FOLLOWERS_DB = new HashMap<String, List<String>>() {{
        put("sally", Arrays.asList("bob", "tim", "alice", "adam", "jim", "chris", "jai"));
        put("bob", Arrays.asList("sally", "nathan", "jim", "mary", "david", "vivian"));
        put("tim", Arrays.asList("alex"));
        put("nathan", Arrays.asList("sally", "bob", "adam", "harry", "chris", "vivian", "emily", "jordan"));
        put("adam", Arrays.asList("david", "carissa"));
        put("mike", Arrays.asList("john", "bob"));
        put("john", Arrays.asList("alice", "nathan", "jim", "mike", "bob"));
    }};
    
    public static class GetTweeters extends BaseBasicBolt {
        @Override
        public void execute(Tuple tuple, BasicOutputCollector collector) {
            Object id = tuple.getValue(0);
            String url = tuple.getString(1);
            List<String> tweeters = TWEETERS_DB.get(url);
            if(tweeters!=null) {
                for(String tweeter: tweeters) {
                    collector.emit(new Values(id, tweeter));
                }
            }
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields("id", "tweeter"));
        }        
    }
    
    public static class GetFollowers extends BaseBasicBolt {
        @Override
        public void execute(Tuple tuple, BasicOutputCollector collector) {
            Object id = tuple.getValue(0);
            String tweeter = tuple.getString(1);
            List<String> followers = FOLLOWERS_DB.get(tweeter);
            if(followers!=null) {
                for(String follower: followers) {
                    collector.emit(new Values(id, follower));
                }
            }
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields("id", "follower"));
        }
    }
    
    public static class PartialUniquer extends BaseBatchBolt {
        BatchOutputCollector _collector;
        Object _id;
        Set<String> _followers = new HashSet<String>();
        
        @Override
        public void prepare(Map conf, TopologyContext context, BatchOutputCollector collector, Object id) {
            _collector = collector;
            _id = id;
        }

        @Override
        public void execute(Tuple tuple) {
            _followers.add(tuple.getString(1));
        }
        
        @Override
        public void finishBatch() {
            _collector.emit(new Values(_id, _followers.size()));
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields("id", "partial-count"));
        }
    }
    
    public static class CountAggregator extends BaseBatchBolt {
        BatchOutputCollector _collector;
        Object _id;
        int _count = 0;
        
        @Override
        public void prepare(Map conf, TopologyContext context, BatchOutputCollector collector, Object id) {
            _collector = collector;
            _id = id;
        }

        @Override
        public void execute(Tuple tuple) {
            _count+=tuple.getInteger(1);
        }
        
        @Override
        public void finishBatch() {
            _collector.emit(new Values(_id, _count));
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields("id", "reach"));
        }
    }
    
    public static LinearDRPCTopologyBuilder construct() {
        LinearDRPCTopologyBuilder builder = new LinearDRPCTopologyBuilder("reach");
        builder.addBolt(new GetTweeters(), 4);
        builder.addBolt(new GetFollowers(), 12)
                 .shuffleGrouping();
        builder.addBolt(new PartialUniquer(), 6)
                 .fieldsGrouping(new Fields("id", "follower"));
        builder.addBolt(new CountAggregator(), 3)
                 .fieldsGrouping(new Fields("id")); 
        return builder;
    }
    
    public static void main(String[] args) throws Exception {
        LinearDRPCTopologyBuilder builder = construct();
        
        
        Config conf = new Config();
        
        if(args==null || args.length==0) {
            conf.setMaxTaskParallelism(3);
            LocalDRPC drpc = new LocalDRPC();
            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology("reach-drpc", conf, builder.createLocalTopology(drpc));
            
            String[] urlsToTry = new String[] { "foo.com/blog/1", "engineering.twitter.com/blog/5", "notaurl.com"};
            for(String url: urlsToTry) {
                System.out.println("Reach of " + url + ": " + drpc.execute("reach", url));
            }
            
            cluster.shutdown();
            drpc.shutdown();
        } else {
            conf.setNumWorkers(6);
            StormSubmitter.submitTopology(args[0], conf, builder.createRemoteTopology());
        }
    }
}
