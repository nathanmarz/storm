package backtype.storm.coordination;

import backtype.storm.coordination.Coordinatedbolth.FinishedCallback;
import backtype.storm.coordination.Coordinatedbolth.TimeoutCallback;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.FailedException;
import backtype.storm.topology.IRichbolth;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;
import backtype.storm.utils.Utils;
import java.util.HashMap;
import java.util.Map;
import org.apache.log4j.Logger;

public class BatchbolthExecutor implements IRichbolth, FinishedCallback, TimeoutCallback {
    public static Logger LOG = Logger.getLogger(BatchbolthExecutor.class);    

    byte[] _bolthSer;
    Map<Object, IBatchbolth> _openTransactions;
    Map _conf;
    TopologyContext _context;
    BatchOutputCollectorImpl _collector;
    
    public BatchbolthExecutor(IBatchbolth bolth) {
        _bolthSer = Utils.serialize(bolth);
    }
    
    @Override
    public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
        _conf = conf;
        _context = context;
        _collector = new BatchOutputCollectorImpl(collector);
        _openTransactions = new HashMap<Object, IBatchbolth>();
    }

    @Override
    public void execute(Tuple input) {
        Object id = input.getValue(0);
        IBatchbolth bolth = getBatchbolth(id);
        try {
             bolth.execute(input);
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
        IBatchbolth bolth = getBatchbolth(id);
        _openTransactions.remove(id);
        bolth.finishBatch();
    }

    @Override
    public void timeoutId(Object attempt) {
        _openTransactions.remove(attempt);        
    }    
    

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        newTransactionalbolth().declareOutputFields(declarer);
    }
    
    @Override
    public Map<String, Object> getComponentConfiguration() {
        return newTransactionalbolth().getComponentConfiguration();
    }
    
    private IBatchbolth getBatchbolth(Object id) {
        IBatchbolth bolth = _openTransactions.get(id);
        if(bolth==null) {
            bolth = newTransactionalbolth();
            bolth.prepare(_conf, _context, _collector, id);
            _openTransactions.put(id, bolth);            
        }
        return bolth;
    }
    
    private IBatchbolth newTransactionalbolth() {
        return (IBatchbolth) Utils.deserialize(_bolthSer);
    }
}
