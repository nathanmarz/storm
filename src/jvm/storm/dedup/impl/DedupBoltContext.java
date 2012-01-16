package storm.dedup.impl;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import storm.dedup.DedupConstants;
import storm.dedup.IDedupBolt;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.Tuple;
import backtype.storm.utils.Utils;

public class DedupBoltContext extends DedupContextBase {
  
  private static final Log LOG = LogFactory.getLog(DedupBoltContext.class);
  
  private OutputCollector collector;
  
  private String currentInputID;
  private int outputIndex;
  
  public DedupBoltContext(Map stormConf, TopologyContext context,
      OutputCollector collector) throws IOException {
    super(stormConf, context);
    
    this.collector = collector;
  }
  
  
  /**
   * DedupSpoutContext specific method
   */
  
  public void execute(IDedupBolt bolt, Tuple input) throws IOException {
    this.outputIndex = 0;
    
    this.currentInputID = input.getStringByField(DedupConstants.TUPLE_ID_FIELD);
    String sourceStream = input.getSourceStreamId();
    LOG.info(getIdentifier() + " receive tuple " + currentInputID
        + " from stream " + sourceStream);
    
    if (DedupConstants.DEDUP_STREAM_ID.equals(sourceStream)) {
      String prefix = currentInputID + DedupConstants.TUPLE_ID_SEP;
      
      List<String> toDelete = new ArrayList<String>();
      for (String tupleid : getOutputMap().keySet()) {
        if (tupleid.startsWith(prefix)) {
          toDelete.add(tupleid);
        }
      }
      for (String tupleid : toDelete) {
        deleteOutput(tupleid);
      }
      
      // remove from persistent store
      saveChanges();
      LOG.info(getIdentifier() + 
            " delete output for input tuple " + currentInputID);
    } else {
      boolean needProcess = true;
      String prefix = currentInputID + DedupConstants.TUPLE_ID_SEP;
      for (String tupleid : getOutputMap().keySet()) {
        if (tupleid.startsWith(prefix)) {
          // just re-send output
          Output output = getOutput(tupleid);
          collector.emit(output.streamId, input, output.tuple);
          LOG.warn(getIdentifier() + " re-emit output for input tuple " + 
              tupleid + " to stream " + output.streamId);
          needProcess = false;
        }
      }
      
      if (needProcess) {
        // call user bolt
        bolt.execute(this, input);
        
        // persistent user bolt set state and output tuple
        saveChanges();
        
        // really emit output
        for (Map.Entry<String, Output> entry : getNewOutput().entrySet()) {
          collector.emit(entry.getValue().streamId, input, 
              entry.getValue().tuple);
          LOG.info(getIdentifier() + 
              " real emit tuple " + getTupleID(entry.getValue().tuple) + 
              " to stream " + entry.getValue().streamId);
        }
      }
    }

    // ack the input tuple
    collector.ack(input);
    LOG.info(getIdentifier() + " ack input tuple " + currentInputID);
    
    clearChanges();
  }


  @Override
  public String getNextTupleID() {
    String tupleid = currentInputID + DedupConstants.TUPLE_ID_SEP +
      getIdentifier() + DedupConstants.TUPLE_ID_SUB_SEP + outputIndex;
    outputIndex++;
    return tupleid;
  }
}
