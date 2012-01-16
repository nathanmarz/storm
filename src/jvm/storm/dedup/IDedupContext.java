package storm.dedup;

import java.util.List;

/**
 * the interface that user spout/bolt should use to 
 * <ul>
 *   <li>get configuration</li>
 *   <li>set or get persistent state</li>
 *   <li>emit tuple</li>
 * </ul>
 *
 */
public interface IDedupContext {
  /**
   * get configuration
   * @param key configuration name
   * @return configuration value
   */
  public Object getConf(Object key);
  
  /**
   * get state by key
   * @param key
   * @return
   */
  public byte[] getState(byte[] key);
  /**
   * set state by key
   * @param key
   * @param value
   * @return
   */
  public void setState(byte[] key, byte[] value);
 
  /**
   * emit a tuple to default stream
   * @param tuple
   */
  public void emit(List<Object> tuple);
  /** 
   * emit a tuple to specific stream
   * @param streamId
   * @param tuple
   */
  public void emit(String streamId, List<Object> tuple);
}
