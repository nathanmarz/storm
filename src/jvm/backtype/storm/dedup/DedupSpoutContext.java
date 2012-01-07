package backtype.storm.dedup;

import java.io.IOException;
import java.io.Serializable;
import java.io.UnsupportedEncodingException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;

public class DedupSpoutContext implements IDedupContext {
  
  private static final Log LOG = LogFactory.getLog(DedupSpoutContext.class);

  private Map<String, String> conf;
  private TopologyContext context;
  private SpoutOutputCollector collector;
  
  private String uniqID;
  
  private AtomicLong globalID;
  
  /**
   * key-value state
   */
  private IStateStore stateStore;
  private byte[] storeKey;
  
  private Map<BytesArrayRef, byte[]> stateMap;
  private Map<BytesArrayRef, byte[]> newState;
  
  private static class Output implements Serializable {
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
    
    this.uniqID = context.getThisComponentId() + ":" + context.getThisTaskId();
    
    this.stateMap = new HashMap<BytesArrayRef, byte[]>();
    this.newState = new HashMap<BytesArrayRef, byte[]>();
    this.outputMap = new HashMap<Long, Output>();
    this.newOutput = new HashMap<Long, Output>();
    

    this.storeKey = Bytes.toBytes(uniqID);
    
    this.stateStore = new HBaseStateStore(context.getStormId());
    stateStore.open();
    // read saved data
    Map<byte[], Map<byte[], byte[]>> storeMap = stateStore.get(storeKey);
    if (storeMap != null) {
      for (Entry<byte[], Map<byte[], byte[]>> entry : storeMap.entrySet()) {
        if (Arrays.equals(IStateStore.STATEMAP, entry.getKey())) {
          for (Entry<byte[], byte[]> stateEntry : entry.getValue().entrySet()) {
            stateMap.put(new BytesArrayRef(stateEntry.getKey()), 
                stateEntry.getValue());
            LOG.info(uniqID + " load STATEMAP");
          }
        } else if (Arrays.equals(IStateStore.OUTPUTMAP, entry.getKey())) {
          for (Entry<byte[], byte[]> outEntry : entry.getValue().entrySet()) {
            outputMap.put(Long.parseLong(Bytes.toString(outEntry.getKey())), 
                (Output)Utils.deserialize(outEntry.getValue()));
            LOG.info(uniqID + " load OUTPUTMAP");
          }
        } else {
          LOG.warn(uniqID + " unknown state " + Bytes.toString(entry.getKey()));
        }
      }
    }
  }
  
  
  /**
   * DedupSpoutContext specific method
   */
  
  public void nextTuple(IDedupSpout spout) throws IOException {
    // call user spout
    spout.nextTuple(this);
    
    // persistent user bolt set state and output tuple
    if (newState.size() > 0 || newOutput.size() > 0) {
      Map<byte[], Map<byte[], byte[]>> updateMap = 
        new HashMap<byte[], Map<byte[], byte[]>>();
      if (newState.size() > 0) {
        Map<byte[], byte[]> map = new HashMap<byte[], byte[]>();
        for (Map.Entry<BytesArrayRef, byte[]> entry : newState.entrySet()) {
          map.put(entry.getKey().getBytes(), entry.getValue());
        }
        updateMap.put(IStateStore.STATEMAP, map);
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
      LOG.info(uniqID + " store " + globalID.get());
      updateMap.clear();
    }
    
    // then really emit tuple
    for (Map.Entry<Long, Output> entry : newOutput.entrySet()) {
      collector.emit(entry.getValue().streamId, 
                     entry.getValue().tuple,
                     entry.getKey());
      LOG.info(uniqID + " real emit tuple " + entry.getKey() + " to " + 
          entry.getValue().streamId);
    }
    
    // clean new state and output buffer
    newState.clear();
    newOutput.clear();
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
      
      LOG.info(uniqID + " acked known message " + messageId);
    } else {
      LOG.warn(uniqID + " not ack unknown message " + messageId);
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
      LOG.warn(uniqID + " re emit tuple " + tupleId + " and message id switch " 
          + messageId + " -> " + newMessageId);
    } else {
      LOG.warn(uniqID + " NOT fail unknown message " + messageId);
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
    LOG.info(uniqID + " user emit tuple " + tupleid + " to stream " + streamId);
  }

  @Override
  public String getConf(String key) {
    return conf.get(key);
  }
  
  @Override
  public byte[] getConf(byte[] key) {
    return Bytes.toBytes(conf.get(Bytes.toString(key)));
  }

  @Override
  public byte[] getState(byte[] key) {
    return stateMap.get(new BytesArrayRef(key));
  }

  @Override
  public boolean setState(byte[] key, byte[] value) {
    newState.put(new BytesArrayRef(key), value);
    stateMap.put(new BytesArrayRef(key), value);
    LOG.info(uniqID + " set state " + 
        Bytes.toString(key) + " : " + Bytes.toString(value));
    return true;
  }
}
