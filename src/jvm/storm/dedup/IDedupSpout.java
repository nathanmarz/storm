package storm.dedup;

import java.io.Serializable;

import backtype.storm.topology.OutputFieldsDeclarer;

/**
 * the interface that user spout should implement.
 */
public interface IDedupSpout extends Serializable {
  /**
   * declare output fields
   * @param context execution context
   */
  void declareOutputFields(OutputFieldsDeclarer declarer);
  
  /**
   * start spout
   * @param context execution context
   */
  void open(IDedupContext context);
  
  /**
   * stop spout
   * @param context execution context
   */
  void close(IDedupContext context);
  
  /**
   * get a net tuple
   * 
   * user bolt can 
   *  use context.setState() to modify bolt state
   *  use context.emit(tuple) to emit a tuple
   *  
   * @param context execution context
   */
  void nextTuple(IDedupContext context);
  
  /**
   * message fully handled
   * @param messageId
   */
  void ack(String messageId);
  
  /**
   * message not fully handle in timeout
   * @param messageId
   */
  void fail(String messageId);
}
