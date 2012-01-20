package backtype.storm.transactional;

import backtype.storm.Config;
import backtype.storm.coordination.CoordinatedBolt.FinishedCallback;
import backtype.storm.coordination.FinishedTuple;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;
import backtype.storm.utils.TimeCacheMap;
import backtype.storm.utils.Utils;
import java.util.Map;

public class TransactionalBoltExecutor implements IRichBolt, FinishedCallback {
    byte[] _boltSer;
    TimeCacheMap<TransactionAttempt, OpenTransaction> _openTransactions;
    Map _conf;
    TopologyContext _context;
    TransactionalOutputCollectorImpl _collector;
    
    public TransactionalBoltExecutor(ITransactionalBolt bolt) {
        _boltSer = Utils.serialize(bolt);
    }
    
    @Override
    public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
        _conf = conf;
        _context = context;
        _collector = new TransactionalOutputCollectorImpl(collector);
        _openTransactions = new TimeCacheMap<TransactionAttempt, OpenTransaction>(Utils.getInt(conf.get(Config.TOPOLOGY_MESSAGE_TIMEOUT_SECS)));
    }

    @Override
    public void execute(Tuple input) {
        TransactionAttempt attempt = (TransactionAttempt) input.getValue(0);
        String stream = input.getSourceStreamId();
        OpenTransaction tx = _openTransactions.get(attempt);
        // bolt != null && receiving commit message -> this task processed the whole batch for this attempt
        // this is because: commit message only sent when tuple tree acked
        // if it failed after, then bolt will equal null, if it failed before, then it doesn't get acked
        //
        // this task processed the whole batch for this attempt && receiving commit message-> bolt != null
        // because the batch tuple is sent, guaranteeing that it sees at least one tuple for the batch
        if(stream.equals(TransactionalSpoutCoordinator.TRANSACTION_COMMIT_STREAM_ID)) {
                if(tx!=null && tx.finished) {
                    ((ICommittable)tx.bolt).commit();
                    _collector.ack(input);
                    _openTransactions.remove(attempt);
                } else {
                    _collector.fail(input);
                }
        } else {
            if(tx==null) {
                tx = new OpenTransaction();
                tx.bolt.prepare(_conf, _context, _collector, attempt);
                _openTransactions.put(attempt, tx);            
            }

            // it is sent the batch id to guarantee that it creates the TransactionalBolt for the attempt before commit
            if(!stream.equals(TransactionalSpoutCoordinator.TRANSACTION_BATCH_STREAM_ID)) {
                tx.bolt.execute(input);
            }       
            _collector.ack(input);
        }
    }

    @Override
    public void cleanup() {
    }

    @Override
    public void finishedId(FinishedTuple tup) {
        TransactionAttempt attempt = (TransactionAttempt) tup.getId();
        OpenTransaction tx = _openTransactions.get(attempt);
        // can equal null if the TimeCacheMap expired it
        if(tx!=null) {
            if(!(tx instanceof ICommittable)) {
                _openTransactions.remove(attempt);
            }
            tx.bolt.finishBatch();
            tx.finished = true;
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        newTransactionalBolt().declareOutputFields(declarer);
    }
    
    @Override
    public Map<String, Object> getComponentConfiguration() {
        return newTransactionalBolt().getComponentConfiguration();
    }
    
    private class OpenTransaction {
        public boolean finished = false;
        public ITransactionalBolt bolt = newTransactionalBolt();
    }

    private ITransactionalBolt newTransactionalBolt() {
        return (ITransactionalBolt) Utils.deserialize(_boltSer);
    }
}
