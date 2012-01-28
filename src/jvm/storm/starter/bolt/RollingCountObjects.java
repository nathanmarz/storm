package storm.starter.bolt;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class RollingCountObjects extends BaseRichBolt {

    private HashMap<Object, long[]> _objectCounts = new HashMap<Object, long[]>();
    private int _numBuckets;
    private transient Thread cleaner;
    private OutputCollector _collector;
    private int _trackMinutes;

    public RollingCountObjects(int numBuckets, int trackMinutes) {
        _numBuckets = numBuckets;
        _trackMinutes = trackMinutes;
    }

    public long totalObjects (Object obj) {
        long[] curr = _objectCounts.get(obj);
        long total = 0;
        for (long l: curr) {
            total+=l;
        }
        return total;
    }

    public int currentBucket (int buckets) {
        return (currentSecond()  / secondsPerBucket(buckets)) % buckets;
    }

    public int currentSecond() {
        return (int) (System.currentTimeMillis() / 1000);
    }

    public int secondsPerBucket(int buckets) {
        return (_trackMinutes * 60 / buckets);
    }

    public long millisPerBucket(int buckets) {
        return (long) secondsPerBucket(buckets) * 1000;
    }

    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        _collector = collector;
        cleaner = new Thread(new Runnable() {
            public void run() {
                Integer lastBucket = currentBucket(_numBuckets);


                while(true) {
                  int currBucket = currentBucket(_numBuckets);
                  if(currBucket!=lastBucket) {
                      int bucketToWipe = (currBucket + 1) % _numBuckets;
                      synchronized(_objectCounts) {
                          Set objs = new HashSet(_objectCounts.keySet());
                          for (Object obj: objs) {
                            long[] counts = _objectCounts.get(obj);
                            long currBucketVal = counts[bucketToWipe];
                            counts[bucketToWipe] = 0;
                            long total = totalObjects(obj);
                            if(currBucketVal!=0) {
                                _collector.emit(new Values(obj, total));
                            }
                            if(total==0) {
                                _objectCounts.remove(obj);
                            }
                          }
                      }
                      lastBucket = currBucket;
                  }
                  long delta = millisPerBucket(_numBuckets) - (System.currentTimeMillis() % millisPerBucket(_numBuckets));
                  Utils.sleep(delta);
                }
            }
        });
        cleaner.start();
    }

    public void execute(Tuple tuple) {

        Object obj = tuple.getValue(0);
        int bucket = currentBucket(_numBuckets);
        synchronized(_objectCounts) {
            long[] curr = _objectCounts.get(obj);
            if(curr==null) {
                curr = new long[_numBuckets];
                _objectCounts.put(obj, curr);
            }
            curr[bucket]++;
            _collector.emit(new Values(obj, totalObjects(obj)));
            _collector.ack(tuple);
        }
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("obj", "count"));
    }
    
}
