package backtype.storm.dedup;

import java.util.Map;

/**
 * a special key-value state store share the bigtable data model.
 *  key : column family : column => value
 *
 */
public interface IStateStore {
  
  public static final String STATEMAP = "STATEMAP";
  public static final String OUTPUTMAP = "OUTPUTMAP";
  
  /**
   * get value map
   * 
   * @param key key-value pair key
   * @return
   */
  public Map<String, Map<String, String>> get(String key);
  
  /**
   * update a key-value
   * 
   * @param key key-value pair key
   * @param valueMap to update value map
   */
  public void set(String key, Map<String, Map<String, String>> valueMap);
  
  /**
   * delete a key-value
   * 
   * @param key key-value key
   * @param valueMap to delete value map
   */
  public void delete(String key, Map<String, Map<String, String>> valueMap);
}
