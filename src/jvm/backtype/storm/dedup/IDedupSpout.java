package backtype.storm.dedup;

import backtype.storm.topology.IComponent;

public interface IDedupSpout {
  /**
   * declare output fields
   * @param context execution context
   */
  void declareOutputFields(IDedupContext context);
  
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
  void ack(long messageId);
  
  /**
   * message not fully handle in timeout
   * @param messageId
   */
  void fail(long messageId);
}
