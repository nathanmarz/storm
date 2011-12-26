package backtype.storm.dedup;

import java.util.List;

import backtype.storm.topology.OutputFieldsDeclarer;

public interface IDedupContext extends OutputFieldsDeclarer {
  /**
   * get configuration
   * @param confName configuration name
   * @return configuration value
   */
  public String getConf(String confName);
  
  /**
   * get state by key
   * @param key
   * @return
   */
  public String getState(String key);
  /**
   * set state by key
   * @param key
   * @param value
   * @return
   */
  public boolean setState(String key, String value);
 
  /**
   * emit a tuple
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
