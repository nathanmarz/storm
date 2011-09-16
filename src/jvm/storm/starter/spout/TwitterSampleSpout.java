package storm.starter.spout;

import twitter4j.conf.ConfigurationBuilder;
import twitter4j.TwitterStream;
import twitter4j.TwitterStreamFactory;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.utils.Utils;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;
import twitter4j.Status;
import twitter4j.StatusDeletionNotice;
import twitter4j.StatusListener;
import static backtype.storm.utils.Utils.*;

public class TwitterSampleSpout implements IRichSpout {
    SpoutOutputCollector _collector;
    LinkedBlockingQueue<String> queue = null;
    TwitterStream _twitterStream;
    String _username;
    String _pwd;
    
    
    public TwitterSampleSpout(String username, String pwd) {
        _username = username;
        _pwd = pwd;
    }
    
    @Override
    public boolean isDistributed() {
        return false;
    }

    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        queue = new LinkedBlockingQueue<String>(1000);
        _collector = collector;
        StatusListener listener = new StatusListener() {

            @Override
            public void onStatus(Status status) {
                queue.offer(status.toString());
            }

            @Override
            public void onDeletionNotice(StatusDeletionNotice sdn) {
            }

            @Override
            public void onTrackLimitationNotice(int i) {
            }

            @Override
            public void onScrubGeo(long l, long l1) {
            }

            @Override
            public void onException(Exception e) {
            }
            
        };
        TwitterStreamFactory fact = new TwitterStreamFactory(new ConfigurationBuilder().setUser(_username).setPassword(_pwd).build());
        _twitterStream = fact.getInstance();
        _twitterStream.addListener(listener);
        _twitterStream.sample();
    }

    @Override
    public void nextTuple() {
        String ret = queue.poll();
        if(ret==null) {
            Utils.sleep(50);
        } else {
            _collector.emit(tuple(ret));            
        }
    }

    @Override
    public void close() {
        _twitterStream.shutdown();
    }

    @Override
    public void ack(Object id) {
    }

    @Override
    public void fail(Object id) {
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("tweet"));
    }
    
}
