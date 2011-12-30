package backtype.storm.dedup;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.UnsupportedEncodingException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;

public class DedupSpoutContext implements IDedupContext {
  
  private static final Log LOG = LogFactory.getLog(DedupSpoutContext.class);

  private Map<String, String> conf;
  private TopologyContext context;
  private SpoutOutputCollector collector;
  
  private AtomicLong globalID;
  
  /**
   * key-value state
   */
  private IStateStore stateStore;
  private byte[] storeKey;
  
  private Map<byte[], byte[]> stateMap;
  private Map<byte[], byte[]> newState;
  
  private static class Output {
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
      SpoutOutputCollector collector) throws IOException {
    this.conf = conf;
    this.context = context;
    this.collector = collector;
    
    this.globalID = new AtomicLong(0);
    
    this.stateMap = new HashMap<byte[], byte[]>();
    this.newState = new HashMap<byte[], byte[]>();
    this.outputMap = new HashMap<Long, Output>();
    this.newOutput = new HashMap<Long, Output>();
    

    this.storeKey = Bytes.toBytes(context.getThisComponentId());
    
    this.stateStore = new HBaseStateStore(context.getStormId());
    stateStore.open();
    // read saved data
    Map<byte[], Map<byte[], byte[]>> storeMap = stateStore.get(storeKey);
    if (storeMap != null) {
      Map<byte[], byte[]> map = storeMap.get(IStateStore.STATEMAP);
      if (map != null) {
        for (Entry<byte[], byte[]> entry : map.entrySet()) {
          stateMap.put(entry.getKey(), entry.getValue());
        }
      }
      
      map = storeMap.get(IStateStore.OUTPUTMAP);
      if (map != null) {
        for (Entry<byte[], byte[]> entry : map.entrySet()) {
          outputMap.put(Long.parseLong(Bytes.toString(entry.getKey())), 
              (Output)Utils.deserialize(entry.getValue()));
        }
      }
    }
  }
  
  
  /**
   * DedupSpoutContext specific method
   */
  
  public void nextTuple(IDedupSpout spout) throws IOException {
    newState.clear();
    newOutput.clear();
    
    // call user spout
    spout.nextTuple(this);
    
    // persistent user bolt set state and output tuple
    if (newState.size() > 0 || newOutput.size() > 0) {
      Map<byte[], Map<byte[], byte[]>> updateMap = 
        new HashMap<byte[], Map<byte[], byte[]>>();
      if (newState.size() > 0) {
        updateMap.put(IStateStore.STATEMAP, newState);
      }
      if (newOutput.size() > 0) {
        Map<byte[], byte[]> map = new HashMap<byte[], byte[]>();
        for (Map.Entry<Long, Output> entry : newOutput.entrySet()) {
          map.put(Bytes.toBytes(entry.getKey().toString()), 
              Utils.serialize(entry.getValue()));
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
  
  public void ack(Object msgId) throws IOException {
    Long messageId = (Long)msgId;
    if (outputMap.containsKey(messageId)) {
      List<Object> tuple = outputMap.remove(messageId).tuple;
      String tupleId = (String)tuple.get(tuple.size() - 2);
      Values notice =
        new Values(tupleId, DedupConstants.TUPLE_TYPE.NOTICE.toString());
      collector.emit(DedupConstants.DEDUP_STREAM_ID, notice);
      
      // delete from state store
      Map<byte[], Map<byte[], byte[]>> deleteMap = 
        new HashMap<byte[], Map<byte[], byte[]>>();
      Map<byte[], byte[]> delete = new HashMap<byte[], byte[]>();
      delete.put(Bytes.toBytes(messageId.toString()), 
          tuple.toString().getBytes()); // TODO
      deleteMap.put(IStateStore.OUTPUTMAP, delete);
      stateStore.delete(storeKey, deleteMap);
      
      LOG.debug("acked known message " + messageId);
    } else {
      LOG.warn("not ack unknown message " + messageId);
    }
    // TODO ack for NOTICE message ?
  }
  
  public void fail(Object msgId) throws IOException {
    Long messageId = (Long)msgId;
    if (outputMap.containsKey(messageId)) {
      Output item = outputMap.remove(messageId);
      // new message id
      Long newMessageId = globalID.getAndIncrement();
      outputMap.put(newMessageId, item);
      // TODO persistent to KV store
      Map<byte[], Map<byte[], byte[]>> updateMap = 
        new HashMap<byte[], Map<byte[], byte[]>>();
      Map<byte[], byte[]> update = new HashMap<byte[], byte[]>();
      update.put(Bytes.toBytes(messageId.toString()), new byte[0]); // TODO
      update.put(Bytes.toBytes(newMessageId.toString()), Utils.serialize(item));
      updateMap.put(IStateStore.OUTPUTMAP, update);
      stateStore.set(storeKey, updateMap);
      
      // re-send tuple use new message id
      collector.emit(item.streamId, item.tuple, newMessageId);
      String tupleId = (String)item.tuple.get(item.tuple.size() - 2);
      LOG.warn("re-send tuple " + tupleId + " and message id switch " + 
          messageId + " -> " + newMessageId);
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
  public String getConf(String key) {
    return conf.get(key);
  }
  
  @Override
  public byte[] getConf(byte[] key) {
    try {
      return conf.get(new String(key, "UTF8")).getBytes("UTF8");
    } catch (UnsupportedEncodingException e) {
      return null;
    }
  }

  @Override
  public byte[] getState(byte[] key) {
    return stateMap.get(key);
  }

  @Override
  public boolean setState(byte[] key, byte[] value) {
    newState.put(key, value);
    stateMap.put(key, value);
    return true;
  }
}
