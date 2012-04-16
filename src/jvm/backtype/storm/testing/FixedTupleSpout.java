package backtype.storm.testing;

import backtype.storm.spout.ISpout;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.utils.Utils;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import static backtype.storm.utils.Utils.get;

public class FixedTupleSpout implements ISpout {
    private static final Map<String, Integer> acked = new HashMap<String, Integer>();
    private static final Map<String, Integer> failed = new HashMap<String, Integer>();

    public static int getNumAcked(String stormId) {
        synchronized(acked) {
            return get(acked, stormId, 0);
        }
    }

    public static int getNumFailed(String stormId) {
        synchronized(failed) {
            return get(failed, stormId, 0);
        }
    }
    
    public static void clear(String stormId) {
        acked.remove(stormId);
        failed.remove(stormId);
    }

    private List<FixedTuple> _tuples;
    private SpoutOutputCollector _collector;

    private TopologyContext _context;
    private List<FixedTuple> _serveTuples;
    private Map<String, FixedTuple> _pending;

    private String _id;
    
    public FixedTupleSpout(List tuples) {
        _id = UUID.randomUUID().toString();
        synchronized(acked) {
            acked.put(_id, 0);
        }
        synchronized(failed) {
            failed.put(_id, 0);
        }
        _tuples = new ArrayList<FixedTuple>();
        for(Object o: tuples) {
            FixedTuple ft;
            if(o instanceof FixedTuple) {
                ft = (FixedTuple) o;
            } else {
                ft = new FixedTuple((List) o);
            }
            _tuples.add(ft);
        }
    }

    public List<FixedTuple> getSourceTuples() {
        return _tuples;
    }
    
    public int getCompleted() {
        int ackedAmt;
        int failedAmt;
        
        synchronized(acked) {
            ackedAmt = acked.get(_id);
        }
        synchronized(failed) {
            failedAmt = failed.get(_id);
        }
        return ackedAmt + failedAmt;
    }
    
    public void cleanup() {
        synchronized(acked) {            
            acked.remove(_id);
        } 
        synchronized(failed) {            
            failed.remove(_id);
        }
    }

    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        _context = context;
        List<Integer> tasks = context.getComponentTasks(context.getThisComponentId());
        int startIndex;
        for(startIndex=0; startIndex<tasks.size(); startIndex++) {
            if(tasks.get(startIndex)==context.getThisTaskId()) {
                break;
            }
        }
        _collector = collector;
        _pending = new HashMap<String, FixedTuple>();
        _serveTuples = new ArrayList<FixedTuple>();
        for(int i=startIndex; i<_tuples.size(); i+=tasks.size()) {
            _serveTuples.add(_tuples.get(i));
        }
    }

    public void close() {
    }

    public void nextTuple() {
        if(_serveTuples.size()>0) {
            FixedTuple ft = _serveTuples.remove(0);
            String id = UUID.randomUUID().toString();
            _pending.put(id, ft);
            _collector.emit(ft.stream, ft.values, id);
        } else {
            Utils.sleep(100);
        }
    }

    public void ack(Object msgId) {
        synchronized(acked) {
            int curr = get(acked, _id, 0);
            acked.put(_id, curr+1);
        }
    }

    public void fail(Object msgId) {
        synchronized(failed) {
            int curr = get(failed, _id, 0);
            failed.put(_id, curr+1);
        }
    }

    @Override
    public void activate() {
    }

    @Override
    public void deactivate() {
    }
}
