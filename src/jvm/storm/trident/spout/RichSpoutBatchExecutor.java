package storm.trident.spout;

import backtype.storm.spout.ISpoutOutputCollector;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichSpout;
import backtype.storm.tuple.Fields;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;
import storm.trident.operation.TridentCollector;
import storm.trident.util.TridentUtils;

public class RichSpoutBatchExecutor implements IBatchSpout {
    public static final String MAX_BATCH_SIZE_CONF = "topology.spout.max.batch.size";

    IRichSpout _spout;
    int _maxBatchSize;
    boolean prepared = false;
    Map _conf;
    TopologyContext _context;
    CaptureCollector _collector;
    TreeMap<Long, List<Object>> idsMap = new TreeMap();
    
    public RichSpoutBatchExecutor(IRichSpout spout) {
        _spout = spout;
    }
    
    @Override
    public void open(Map conf, TopologyContext context) {
        Number batchSize = (Number) conf.get(MAX_BATCH_SIZE_CONF);
        if(batchSize==null) batchSize = 1000;
        _maxBatchSize = batchSize.intValue();
        _collector = new CaptureCollector();
    }

    @Override
    public void emitBatch(long txid, TridentCollector collector) {
        _collector.reset(collector);
        SortedMap<Long, List<Object>> toFail = idsMap.tailMap(txid);
        for(long failTx: idsMap.keySet()) {
            for(Object id: idsMap.get(failTx)) {
                _spout.fail(id);
            }
        }
        toFail.clear();
        if(!prepared) {
            _spout.open(_conf, _context, new SpoutOutputCollector(_collector));
            prepared = true;
        }
        for(int i=0; i<_maxBatchSize; i++) {
            _spout.nextTuple();
            if(_collector.numEmitted < i) {
                break;
            }
            idsMap.put(txid, _collector.ids);
        }
    }
    
    @Override
    public void ack(long batchId) {
        List<Object> ids = idsMap.remove(batchId);
        if(ids!=null) {
            for(Object id: ids) {
                _spout.ack(id);
            }
        }
    }    

    @Override
    public void close() {
        _spout.close();
    }

    @Override
    public Map getComponentConfiguration() {
        return _spout.getComponentConfiguration();
    }

    @Override
    public Fields getOutputFields() {
        return TridentUtils.getSingleOutputStreamFields(_spout);
        
    }
    
    static class CaptureCollector implements ISpoutOutputCollector {

        TridentCollector _collector;
        public List<Object> ids;
        public int numEmitted;
        
        public void reset(TridentCollector c) {
            _collector = c;
            ids = new ArrayList<Object>();
        }
        
        @Override
        public void reportError(Throwable t) {
            _collector.reportError(t);
        }

        @Override
        public List<Integer> emit(String stream, List<Object> values, Object id) {
            if(id!=null) ids.add(id);
            numEmitted++;            
            _collector.emit(values);
            return null;
        }

        @Override
        public void emitDirect(int task, String stream, List<Object> values, Object id) {
            throw new UnsupportedOperationException("Trident does not support direct streams");
        }
        
    }
    
}
