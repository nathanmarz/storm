package backtype.storm.coordination;

import backtype.storm.coordination.CoordinatedBolt.FinishedCallback;
import backtype.storm.coordination.CoordinatedBolt.TimeoutCallback;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.FailedException;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;
import backtype.storm.utils.Utils;
import java.util.HashMap;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BatchBoltExecutor implements IRichBolt, FinishedCallback, TimeoutCallback {
    public static Logger LOG = LoggerFactory.getLogger(BatchBoltExecutor.class);    

    byte[] _boltSer;
    Map<Object, IBatchBolt> _openTransactions;
    Map _conf;
    TopologyContext _context;
    BatchOutputCollectorImpl _collector;
    
    public BatchBoltExecutor(IBatchBolt bolt) {
        _boltSer = Utils.serialize(bolt);
    }
    
    @Override
    public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
        _conf = conf;
        _context = context;
        _collector = new BatchOutputCollectorImpl(collector);
        _openTransactions = new HashMap<Object, IBatchBolt>();
    }

    @Override
    public void execute(Tuple input) {
        Object id = input.getValue(0);
        IBatchBolt bolt = getBatchBolt(id);
        try {
             bolt.execute(input);
            _collector.ack(input);
        } catch(FailedException e) {
            LOG.error("Failed to process tuple in batch", e);
            _collector.fail(input);                
        }
    }

    @Override
    public void cleanup() {
    }

    @Override
    public void finishedId(Object id) {
        IBatchBolt bolt = getBatchBolt(id);
        _openTransactions.remove(id);
        bolt.finishBatch();
    }

    @Override
    public void timeoutId(Object attempt) {
        _openTransactions.remove(attempt);        
    }    
    

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        newTransactionalBolt().declareOutputFields(declarer);
    }
    
    @Override
    public Map<String, Object> getComponentConfiguration() {
        return newTransactionalBolt().getComponentConfiguration();
    }
    
    private IBatchBolt getBatchBolt(Object id) {
        IBatchBolt bolt = _openTransactions.get(id);
        if(bolt==null) {
            bolt = newTransactionalBolt();
            bolt.prepare(_conf, _context, _collector, id);
            _openTransactions.put(id, bolt);            
        }
        return bolt;
    }
    
    private IBatchBolt newTransactionalBolt() {
        return (IBatchBolt) Utils.deserialize(_boltSer);
    }
}
