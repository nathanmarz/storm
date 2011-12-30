package backtype.storm.dedup;

import java.io.IOException;
import java.util.Map;

/**
 * a special key-value state store share the bigtable data model.
 *  key : column family : column => value
 *
 */
public interface IStateStore {
  
  public static final byte[] STATEMAP = Bytes.toBytes("STATE");
  public static final byte[] OUTPUTMAP = Bytes.toBytes("OUTPUT");
  
  /**
   * open the state store
   * 
   * @throws IOException
   */
  public void open() throws IOException;
  
  /**
   * close the state sotre
   * 
   * @throws IOException
   */
  public void close() throws IOException;
  
  /**
   * get value map
   * 
   * @param key key-value pair key
   * @return
   */
  public Map<byte[], Map<byte[], byte[]>> get(byte[] key)
  throws IOException;
  
  /**
   * update a key-value
   * 
   * @param key key-value pair key
   * @param valueMap to update value map
   */
  public void set(byte[] key, Map<byte[], Map<byte[], byte[]>> valueMap)
  throws IOException;
  
  /**
   * delete a key-value
   * 
   * @param key key-value key
   * @param valueMap to delete value map
   */
  public void delete(byte[] key, Map<byte[], Map<byte[], byte[]>> valueMap)
  throws IOException;
}
