package backtype.storm.dedup;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;

public class DedupSpoutContext implements IDedupContext {
  
  private static final Log LOG = LogFactory.getLog(DedupSpoutContext.class);

  private Map<String, String> conf;
  private TopologyContext context;
  private SpoutOutputCollector collector;
  private OutputFieldsDeclarer declarer;
  
  private AtomicLong globalID;
  
  /**
   * key-value state
   */
  private IStateStore stateStore;
  private String storeKey;
  
  private Map<String, String> stateMap;
  private Map<String, String> newState;
  
  private class Output {
    public Output(String streamId, List<Object> tuple) {
      this.streamId = streamId;
      this.tuple = tuple;
    }
    
    public String streamId;
    public List<Object> tuple;
  }
  /**
   * message id => tuple
   */
  private Map<Long, Output> outputMap;
  private Map<Long, Output> newOutput;
  
  public DedupSpoutContext(Map conf, TopologyContext context,
      SpoutOutputCollector collector) {
    this.conf = conf;
    this.context = context;
    this.collector = collector;
    
    this.globalID = new AtomicLong(0);
    
    this.stateStore = new HBaseStateStore();
    this.storeKey = context.getThisComponentId();
    
    this.stateMap = new HashMap<String, String>();
    this.newState = new HashMap<String, String>();
    this.outputMap = new HashMap<Long, Output>();
    this.newOutput = new HashMap<Long, Output>();
  }
  
  
  /**
   * DedupSpoutContext specific method
   */
  
  public void setOutputFieldsDeclarer(OutputFieldsDeclarer declarer) {
    this.declarer = declarer;
  }
  
  public void beforeNextTuple() {
    newState.clear();
    newOutput.clear();
  }
  
  public void afterNextTuple() {
    // persistent user bolt set state and output tuple
    if (newState.size() > 0 || newOutput.size() > 0) {
      Map<String, Map<String, String>> updateMap = 
        new HashMap<String, Map<String, String>>();
      if (newState.size() > 0) {
        updateMap.put(IStateStore.STATEMAP, newState);
      }
      if (newOutput.size() > 0) {
        Map<String, String> map = new HashMap<String, String>();
        for (Map.Entry<Long, Output> entry : newOutput.entrySet()) {
          map.put(entry.getKey().toString(), entry.getValue().toString());
        }
        updateMap.put(IStateStore.OUTPUTMAP, map);
      }
      stateStore.set(storeKey, updateMap);
      updateMap.clear();
    }
    
    // then really emit tuple
    for (Map.Entry<Long, Output> entry : newOutput.entrySet()) {
      collector.emit(entry.getValue().streamId, 
                     entry.getValue().tuple,
                     entry.getKey());
    }
  }
  
  public void ack(Object msgId) {
    Long messageId = (Long)msgId;
    if (outputMap.containsKey(messageId)) {
      List<Object> tuple = outputMap.remove(messageId).tuple;
      String tupleId = (String)tuple.get(tuple.size() - 2);
      Values notice =
        new Values(tupleId, DedupConstants.TUPLE_TYPE.NOTICE.toString());
      collector.emit(DedupConstants.DEDUP_STREAM_ID, notice);
      
      // delete from state store
      Map<String, Map<String, String>> deleteMap = 
        new HashMap<String, Map<String, String>>();
      Map<String, String> delete = new HashMap<String, String>();
      delete.put(messageId.toString(), tuple.toString());
      deleteMap.put(IStateStore.OUTPUTMAP, delete);
      stateStore.delete(storeKey, deleteMap);
      
      LOG.debug("acked known message " + messageId);
    } else {
      LOG.warn("not ack unknown message " + messageId);
    }
    // TODO ack for NOTICE message ?
  }
  
  public void fail(Object msgId) {
    Long messageId = (Long)msgId;
    if (outputMap.containsKey(messageId)) {
      Output item = outputMap.remove(messageId);
      // new message id
      messageId = globalID.getAndIncrement();
      outputMap.put(messageId, item);
      // TODO persistent to KV store
      Map<String, Map<String, String>> updateMap = 
        new HashMap<String, Map<String, String>>();
      Map<String, String> update = new HashMap<String, String>();
      update.put(((Long)msgId).toString(), null);
      update.put(messageId.toString(), item.toString());
      updateMap.put(IStateStore.OUTPUTMAP, update);
      stateStore.delete(storeKey, updateMap);
      
      // re-send tuple use new message id
      collector.emit(item.streamId, item.tuple, messageId);
      String tupleId = (String)item.tuple.get(item.tuple.size() - 2);
      LOG.warn("re-send tuple " + tupleId + " and message id switch " + 
          messageId + " -> " + (Long)msgId);
    } else {
      LOG.warn("not fail unknown message " + messageId);
    }
    // TODO fail for NOTICE message ?
  }
  
  
  
  /**
   * implement IDedupContext method
   */
  
  @Override
  public void emit(List<Object> tuple) {
    emit(Utils.DEFAULT_STREAM_ID, tuple);
  }

  @Override
  public void emit(String streamId, List<Object> tuple) {
    Long tupleid = globalID.getAndIncrement();
    // add tuple id to tuple
    tuple.add(tupleid.toString());
    // add tuple type to tuple
    tuple.add(DedupConstants.TUPLE_TYPE.NORMAL.toString());
    
    // add tuple to new output buffer
    newOutput.put(tupleid, new Output(streamId, tuple));
    outputMap.put(tupleid, new Output(streamId, tuple));
  }

  @Override
  public String getConf(String confName) {
    return conf.get(confName);
  }

  @Override
  public String getState(String key) {
    return stateMap.get(key);
  }

  @Override
  public boolean setState(String key, String value) {
    newState.put(key, value);
    stateMap.put(key, value);
    return true;
  }
  
  
  /**
   * implement OutputFieldsDeclarer method
   */

  @Override
  public void declare(Fields fields) {
    // add tow fields to original fields
    List<String> fieldList = fields.toList();
    fieldList.add(DedupConstants.TUPLE_ID_FIELD);
    fieldList.add(DedupConstants.TUPLE_TYPE_FIELD);
    declarer.declare(new Fields(fieldList));
    // declare DEDUP_STREAM with to fields
    declarer.declareStream(DedupConstants.DEDUP_STREAM_ID, 
        new Fields(DedupConstants.TUPLE_ID_FIELD, 
            DedupConstants.TUPLE_TYPE_FIELD));
  }

  @Override
  public void declare(boolean direct, Fields fields) {
    // TODO Auto-generated method stub
    
  }

  @Override
  public void declareStream(String streamId, Fields fields) {
    // TODO Auto-generated method stub
    
  }

  @Override
  public void declareStream(String streamId, boolean direct, Fields fields) {
    // TODO Auto-generated method stub
    
  }

}
