package storm.dedup.impl;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import storm.dedup.Bytes;
import storm.dedup.DedupConstants;
import storm.dedup.IDedupSpout;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;

public class DedupSpoutContext extends DedupContextBase {
  
  private static final Log LOG = LogFactory.getLog(DedupSpoutContext.class);
  
  private static final String GLOBAL_ID = "global id";

  private SpoutOutputCollector collector;
  
  private AtomicLong globalID;
  
  
  public DedupSpoutContext(Map conf, TopologyContext context,
      SpoutOutputCollector collector) throws IOException {
    super(conf, context);
    this.collector = collector;
    
    byte[] bytes = getSystemState(Bytes.toBytes(GLOBAL_ID));
    if (bytes != null) {
      this.globalID = new AtomicLong(Long.parseLong(Bytes.toString(bytes)));
    } else {
      this.globalID = new AtomicLong(0);
    }
  }
  
  
  /**
   * DedupSpoutContext specific method
   */
  
  public void nextTuple(IDedupSpout spout) throws IOException {
    // call user spout
    spout.nextTuple(this);
    
    // persistent user bolt set state and output tuple befor real emit
    saveChanges();
    
    // then really emit tuple
    for (Map.Entry<String, Output> entry : getNewOutput().entrySet()) {
      collector.emit(entry.getValue().streamId, 
                     entry.getValue().tuple,
                     entry.getKey());
      LOG.info(getIdentifier() + " real emit tuple " + entry.getKey() + 
          " to stream " + entry.getValue().streamId);
    }
    
    // clean new changes buffer
    clearChanges();
  }
  
  public void ack(Object msgId) throws IOException {
    String messageId = (String)msgId;
    Output output = getOutput(messageId);
    if (output != null) {
      String tupleId = getTupleID(output.tuple);
      Values notice = new Values(tupleId);
      collector.emit(DedupConstants.DEDUP_STREAM_ID, notice);
      
      // delete from state store
      deleteOutput(messageId.toString());
      
      // remove from persistent store
      saveChanges();
      LOG.info(getIdentifier() + 
          " delete output for tuple " + tupleId);
      LOG.info(getIdentifier() + " acked known message " + messageId);
    } else {
      LOG.warn(getIdentifier() + " not ack unknown message " + messageId);
    }
    
    // clean new changes buffer
    clearChanges();
    
    // TODO ack for NOTICE message ?
  }
  
  public void fail(Object msgId) throws IOException {
    String messageId = (String)msgId;
    Output output = getOutput(messageId);
    if (output != null) {
      // new message id, just append a char ":"
      String newMessageId = messageId + DedupConstants.COLON;
      emit(output.streamId, output.tuple, newMessageId);
      
      // persistent new tuple to state store before re-emit emit
      saveChanges();
      
      // re-emit tuple use new message id
      collector.emit(output.streamId, output.tuple, newMessageId);
      
      // persistent delete old tuple to state store after re-emit
      deleteOutput(messageId.toString());
      saveChanges();
      
      LOG.warn(getIdentifier() + " re emit tuple " + getTupleID(output.tuple) + 
          " and message id switch " + messageId + " -> " + newMessageId);
    } else {
      LOG.warn(getIdentifier() + " NOT fail unknown message " + messageId);
    }
    
    // clean new changes buffer
    clearChanges();
    
    // TODO fail for NOTICE message ?
  }
  
  @Override
  public String getNextTupleID() {
    String tupleid = getIdentifier() + 
              DedupConstants.TUPLE_ID_SUB_SEP + globalID.getAndIncrement();
    // set globalID to system state
    setSystemState(Bytes.toBytes(GLOBAL_ID), 
        Bytes.toBytes(globalID.toString()));
    return tupleid;
  }
}
