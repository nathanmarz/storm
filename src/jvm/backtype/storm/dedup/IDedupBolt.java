package backtype.storm.dedup;

import backtype.storm.tuple.Tuple;

public interface IDedupBolt {
  /**
   * declare output fields
   * @param context execution context
   */
  void declareOutputFields(IDedupContext context);
  
  /**
   * start bolt
   * @param context execution context
   */
  void prepare(IDedupContext context);
  
  /**
   * stop bolt
   * @param context execution context
   */
  void cleanup(IDedupContext context);
  
  /**
   * process an input tuple
   * 
   * user bolt can 
   *  use context.setState() to modify bolt state
   *  use context.emit(tuple) to emit a tuple
   * @param context execution context
   * @param input input tuple
   */
  void execute(IDedupContext context, Tuple input);
}
