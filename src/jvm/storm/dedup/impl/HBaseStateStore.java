package storm.dedup.impl;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Map.Entry;

import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;

import storm.dedup.IStateStore;

public class HBaseStateStore implements IStateStore {
  
  private HTable table;
  
  public HBaseStateStore(String tableName) throws IOException {
    this.table = new HTable(tableName);
  }


  @Override
  public void open() throws IOException {
    // nothing
  }
  
  @Override
  public void close() throws IOException {
    table.close();
  }
  
  @Override
  public Map<byte[], Map<byte[], byte[]>> get(byte[] key) throws IOException {
    Get get = new Get(key);
    Result result = table.get(get);
    NavigableMap<byte[], NavigableMap<byte[], byte[]>> sortMap = 
      result.getNoVersionMap();
    
    Map<byte[], Map<byte[], byte[]>> map = 
      new HashMap<byte[], Map<byte[],byte[]>>();
    if (sortMap != null) {
      for (Entry<byte[], NavigableMap<byte[], byte[]>> family : 
        sortMap.entrySet()) {
        Map<byte[], byte[]> columnMap = map.get(family.getKey());
        if (columnMap == null) {
          columnMap = new HashMap<byte[], byte[]>();
          map.put(family.getKey(), columnMap);
          for (Entry<byte[], byte[]> column : family.getValue().entrySet()) {
            columnMap.put(column.getKey(), column.getValue());
          }
        }
      }
    }
    
    return map;
  }

  @Override
  public void set(byte[] key, Map<byte[], Map<byte[], byte[]>> valueMap) 
  throws IOException {
    Put put = new Put(key);
    for (Entry<byte[], Map<byte[], byte[]>> family : valueMap.entrySet()) {
      for (Entry<byte[], byte[]> column : family.getValue().entrySet()) {
        put.add(family.getKey(), column.getKey(), column.getValue());
      }
    }
    table.put(put);
  }
  
  @Override
  public void delete(byte[] key, Map<byte[], Map<byte[], byte[]>> valueMap) 
  throws IOException {
    Delete delete = new Delete(key);
    for (Entry<byte[], Map<byte[], byte[]>> family : valueMap.entrySet()) {
      for (Entry<byte[], byte[]> column : family.getValue().entrySet()) {
        delete.deleteColumn(family.getKey(), column.getKey());
      }
    }
    table.delete(delete);
  }
}
