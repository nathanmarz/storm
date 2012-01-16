package storm.dedup.impl;

import java.io.IOException;
import java.io.Serializable;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import storm.dedup.Bytes;
import storm.dedup.DedupConstants;
import storm.dedup.IDedupContext;
import storm.dedup.IStateStore;

import backtype.storm.task.TopologyContext;
import backtype.storm.utils.Utils;

/**
 * Base DedupContext that DedupSpoutContext and DedupBoltContext extends.
 * 
 * It handle the common functions 
 * <ul>
 *   <li>provide getConf(), backed by stormConf map</li>
 *   <li>provide getState()/setState(), backed by state store</li>
 *   <li>create state store with three map : state, output and system</li>
 *   <li>provide saveChanges() for subclass to save changes to state store</li>
 *   <li>load data from state store on start up</li>
 * </ul>
 *
 */
public abstract class DedupContextBase implements IDedupContext {
  /**
   * internal used class Output
   *
   */
  protected static class Output implements Serializable {
    public String streamId;
    public List<Object> tuple;
    
    public Output(String streamId, List<Object> tuple) {
      this.streamId = streamId;
      this.tuple = tuple;
    }
  }
  
  private static final Log LOG = LogFactory.getLog(DedupContextBase.class);
  
  private static final byte[] UPTIME = Bytes.toBytes("uptime");
  private static final byte[] EMIT_COUNT = Bytes.toBytes("emit count");
  
  /**
   * storm conf and context
   */
  private Map conf;
  private TopologyContext context;
  
  /**
   * unique identifier of this task
   */
  private String identifier;
  
  /**
   * state map
   * 
   * key bytes => value bytes
   */  
  private Map<BytesArrayRef, byte[]> stateMap;
  private Map<BytesArrayRef, byte[]> newState;
  
  private Map<BytesArrayRef, byte[]> systemMap;
  private Map<BytesArrayRef, byte[]> newSystem;
  
  
  /**
   * output map
   * 
   * tuple id => tuple output
   */
  private Map<String, Output> outputMap;
  private Map<String, Output> newOutput;

  /**
   * to delete
   */
  private Map<BytesArrayRef, byte[]> newDelete;

  /**
   * state storage
   * 
   * store state, system and output map
   */
  private IStateStore stateStore;
  private byte[] storeKey;
  
  
  public DedupContextBase(Map conf, TopologyContext context) throws IOException {
    this.conf = conf;
    this.context = context;
    
    this.identifier = context.getThisComponentId() + 
      DedupConstants.COLON + context.getThisTaskId();
    
    // crate in-memory map
    this.stateMap = new HashMap<BytesArrayRef, byte[]>();
    this.newState = new HashMap<BytesArrayRef, byte[]>();
    this.systemMap = new HashMap<BytesArrayRef, byte[]>();
    this.newSystem = new HashMap<BytesArrayRef, byte[]>();
    this.outputMap = new HashMap<String, Output>();
    this.newOutput = new HashMap<String, Output>();
    this.newDelete = new HashMap<BytesArrayRef, byte[]>();
    
    // create state store
    this.storeKey = Bytes.toBytes(identifier);
    this.stateStore = new HBaseStateStore((String)conf.get(
        DedupConstants.STATE_STORE_NAME)); // TODO depends on HBase?
    stateStore.open();
    
    // load data from state store to in-memory map
    Map<byte[], Map<byte[], byte[]>> storeMap = stateStore.get(storeKey);
    if (storeMap != null) {
      for (Entry<byte[], Map<byte[], byte[]>> entry : storeMap.entrySet()) {
        if (Arrays.equals(IStateStore.STATEMAP, entry.getKey())) {
          for (Entry<byte[], byte[]> stateEntry : entry.getValue().entrySet()) {
            stateMap.put(new BytesArrayRef(stateEntry.getKey()), 
                stateEntry.getValue());
            LOG.info(identifier + " load STATEMAP " + 
                Bytes.toString(stateEntry.getKey()) + " = \n" +
                Bytes.toString(stateEntry.getValue()));
          }
        } else if (Arrays.equals(IStateStore.SYSTEMMAP, entry.getKey())) {
          for (Entry<byte[], byte[]> systemEntry : entry.getValue().entrySet()) {
            systemMap.put(new BytesArrayRef(systemEntry.getKey()), 
                systemEntry.getValue());
            LOG.info(identifier + " load SYSTEMMAP " + 
                Bytes.toString(systemEntry.getKey()) + " = \n" +
                Bytes.toString(systemEntry.getValue()));
          }
        } else if (Arrays.equals(IStateStore.OUTPUTMAP, entry.getKey())) {
          for (Entry<byte[], byte[]> outEntry : entry.getValue().entrySet()) {
            outputMap.put(Bytes.toString(outEntry.getKey()), 
                (Output)Utils.deserialize(outEntry.getValue()));
            LOG.info(identifier + " load OUTPUTMAP" + 
                Bytes.toString(outEntry.getKey()) + " = \n" +
                Bytes.toString(outEntry.getValue()));
          }
        } else {
          LOG.warn(identifier + " unknown data " + 
              Bytes.toString(entry.getKey()));
        }
      }
    }
    
    // set uptime
    setSystemState(UPTIME, 
        Bytes.toBytes(Long.toString(System.currentTimeMillis())));
    saveChanges();
    clearChanges();
  }
  
  public void close() throws IOException {
    if (stateStore != null)
      stateStore.close();
  }
  
  protected String getIdentifier() {
    return this.identifier;
  }
  
  protected Map<String, Output> getNewOutput() {
    return this.newOutput;
  }
  
  protected Map<String, Output> getOutputMap() {
    return this.outputMap;
  }
  
  protected Output getOutput(String id) {
    return outputMap.get(id);
  }
  
  protected void deleteOutput(String tupleid) {
    Output output = outputMap.remove(tupleid);
    newDelete.put(new BytesArrayRef(Bytes.toBytes(tupleid)), new byte[0]);
  }
  
  protected byte[] getSystemState(byte[] key) {
    return systemMap.get(new BytesArrayRef(key));
  }

  protected void setSystemState(byte[] key, byte[] value) {
    newSystem.put(new BytesArrayRef(key), value);
    systemMap.put(new BytesArrayRef(key), value);
    LOG.info(identifier + " set system state " + 
        Bytes.toString(key) + " : " + Bytes.toString(value));
  }
  
  
  protected void saveChanges() throws IOException {
    if (newState.size() > 0 || newSystem.size() > 0 || newOutput.size() > 0) {
      Map<byte[], Map<byte[], byte[]>> updateMap = 
        new HashMap<byte[], Map<byte[], byte[]>>();
      if (newState.size() > 0) {
        Map<byte[], byte[]> map = new HashMap<byte[], byte[]>();
        for (Map.Entry<BytesArrayRef, byte[]> entry : newState.entrySet()) {
          map.put(entry.getKey().getBytes(), entry.getValue());
        }
        updateMap.put(IStateStore.STATEMAP, map);
      }
      if (newSystem.size() > 0) {
        Map<byte[], byte[]> map = new HashMap<byte[], byte[]>();
        for (Map.Entry<BytesArrayRef, byte[]> entry : newSystem.entrySet()) {
          map.put(entry.getKey().getBytes(), entry.getValue());
        }
        updateMap.put(IStateStore.SYSTEMMAP, map);
      }
      if (newOutput.size() > 0) {
        Map<byte[], byte[]> map = new HashMap<byte[], byte[]>();
        for (Map.Entry<String, Output> entry : newOutput.entrySet()) {
          map.put(Bytes.toBytes(entry.getKey()), 
              Utils.serialize(entry.getValue()));
        }
        updateMap.put(IStateStore.OUTPUTMAP, map);
      }
      stateStore.set(storeKey, updateMap);
      LOG.info(identifier + " saved");
      updateMap.clear();
    }
    
    if (newDelete.size() > 0) {
      Map<byte[], Map<byte[], byte[]>> updateMap = 
        new HashMap<byte[], Map<byte[], byte[]>>();
      Map<byte[], byte[]> map = new HashMap<byte[], byte[]>();
      for (Map.Entry<BytesArrayRef, byte[]> entry : newDelete.entrySet()) {
        map.put(entry.getKey().getBytes(), entry.getValue());
      }
      updateMap.put(IStateStore.OUTPUTMAP, map);
      stateStore.delete(storeKey, updateMap);
    }
  }
  
  protected void clearChanges() {
    newState.clear();
    newSystem.clear();
    newOutput.clear();
    newDelete.clear();
  }


  @Override
  public Object getConf(Object key) {
    return conf.get(key);
  }

  @Override
  public byte[] getState(byte[] key) {
    return stateMap.get(new BytesArrayRef(key));
  }

  @Override
  public void setState(byte[] key, byte[] value) {
    newState.put(new BytesArrayRef(key), value);
    stateMap.put(new BytesArrayRef(key), value);
    LOG.info(identifier + " set state " + 
        Bytes.toString(key) + " : " + Bytes.toString(value));
  }

  @Override
  public void emit(List<Object> tuple) {
    emit(Utils.DEFAULT_STREAM_ID, tuple);
  }

  @Override
  public void emit(String streamId, List<Object> tuple) {
    emit(streamId, tuple, null);
  }
  
  public void emit(String streamId, List<Object> tuple, String tupleid) {
    if (tupleid == null) {
      // get tuple id
      tupleid = getNextTupleID();
      
      // add tuple id to tuple
      tuple.add(tupleid);
    }
    
    // put output to new output buffer
    Output output = new Output(streamId, tuple);
    newOutput.put(tupleid, output);
    outputMap.put(tupleid, output);
    LOG.info(identifier + 
        " user emit tuple " + tupleid + " to stream " + streamId);
    byte[] bytes = getSystemState(EMIT_COUNT);
    Long emitCount = 0L;
    if (bytes == null) {
      emitCount = 0L;
    } else {
      emitCount++;
    }
    setSystemState(EMIT_COUNT, Bytes.toBytes(emitCount.toString()));
  }
  
  protected String getTupleID(List<Object> tuple) {
    return (String) tuple.get(tuple.size() - 1);
  }
  
  protected abstract String getNextTupleID();

}
