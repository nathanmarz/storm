package backtype.storm.dedup;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.utils.Utils;

public class DedupBoltContext implements IDedupContext {
  
  private static final Log LOG = LogFactory.getLog(DedupBoltContext.class);
  
  private Map<String, String> conf;
  private TopologyContext context;
  private OutputCollector collector;
  
  private OutputFieldsDeclarer declarer;
  
  private Tuple currentInput;
  private String currentInputID;
  private int currentOutputIndex;
  
  /**
   * key-value state
   */
  private Map<String, String> stateMap;
  private boolean stateChange;
  
  private class Output {
    public Output(int streamId, List<Object> tuple) {
      this.streamId = streamId;
      this.tuple = tuple;
    }
    
    public int streamId;
    public List<Object> tuple;
  }
  /**
   * message id => tuple
   */
  private Map<String, Output> outputMap;
  private Map<String, Output> newOutput;
  
  public DedupBoltContext(Map stormConf, TopologyContext context,
      OutputCollector collector) {
    this.conf = stormConf;
    this.context = context;
    this.collector = collector;
    
    this.stateMap = new HashMap<String, String>();
    this.outputMap = new HashMap<String, Output>();
    this.newOutput = new HashMap<String, Output>();
  }
  
  
  /**
   * DedupSpoutContext specific method
   */
  
  public void setOutputFieldsDeclarer(OutputFieldsDeclarer declarer) {
    this.declarer = declarer;
  }
  
  public void execute(IDedupBolt bolt, Tuple input) {
    this.currentInput = input;
    this.currentOutputIndex = 0;
    this.stateChange = false;
    this.newOutput.clear();
    
    List<Object> list = input.select(new Fields(DedupConstants.TUPLE_ID_FIELD, 
        DedupConstants.TUPLE_TYPE_FIELD));
    this.currentInputID = (String)list.get(0);
    DedupConstants.TUPLE_TYPE type = 
      DedupConstants.TUPLE_TYPE.valueOf((String)list.get(1));
    if (DedupConstants.TUPLE_TYPE.NORMAL == type) {
      bolt.execute(this, input);
      if (stateChange || newOutput.size() > 0) {
        for (Map.Entry<String, Output> entry : newOutput.entrySet()) {
          outputMap.put(entry.getKey(), entry.getValue());
        }
        // TODO persistent state and output to KV store
        
        // really emit output
        for (Map.Entry<String, Output> entry : newOutput.entrySet()) {
          collector.emit(entry.getValue().streamId, input, 
              entry.getValue().tuple);
        }
        collector.ack(input);
      }
    } else if (DedupConstants.TUPLE_TYPE.DUPLICATE == type) {
      String prefix = currentInputID + DedupConstants.TUPLE_ID_SEP;
      for (String tupleid : outputMap.keySet()) {
        if (tupleid.startsWith(prefix)) {
          // just re-send output
          Output output = outputMap.get(tupleid);
          collector.emit(output.streamId, input, output.tuple);
        }
      }
    } else if (DedupConstants.TUPLE_TYPE.NOTICE == type) {
      String prefix = currentInputID + DedupConstants.TUPLE_ID_SEP;
      for (String tupleid : outputMap.keySet()) {
        if (tupleid.startsWith(prefix)) {
          // TODO to remove
          newOutput.remove(tupleid);
        }
      }
    } else {
      LOG.warn("unknown type " + type);
    }
    
    this.currentInput = null;
  }
  
  
  /**
   * implement IDedupContext method
   */

  @Override
  public void emit(List<Object> tuple) {
    emit(Utils.DEFAULT_STREAM_ID, tuple);
  }

  @Override
  public void emit(int streamId, List<Object> tuple) {
    String tupleid = currentInputID + DedupConstants.TUPLE_ID_SEP + 
      context.getThisComponentId() + 
      DedupConstants.TUPLE_ID_SUB_SEP + 
      currentOutputIndex;
    currentOutputIndex++;
    // add tuple id to tuple
    tuple.add(tupleid.toString());
    // add tuple type to tuple
    tuple.add(DedupConstants.TUPLE_TYPE.NORMAL.toString());
    
    // add tuple to new output buffer
    newOutput.put(tupleid, new Output(streamId, tuple));
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
    stateMap.put(key, value);
    stateChange = true;
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
  }

  @Override
  public void declare(boolean direct, Fields fields) {
    // TODO Auto-generated method stub

  }

  @Override
  public void declareStream(int streamId, Fields fields) {
    // TODO Auto-generated method stub

  }

  @Override
  public void declareStream(int streamId, boolean direct, Fields fields) {
    // TODO Auto-generated method stub

  }

}
