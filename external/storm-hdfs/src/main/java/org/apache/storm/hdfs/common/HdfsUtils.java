package org.apache.storm.hdfs.common;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;

public class HdfsUtils {
  /** list files sorted by modification time that have not been modified since 'olderThan'. if
   * 'olderThan' is <= 0 then the filtering is disabled */
  public static Collection<Path> listFilesByModificationTime(FileSystem fs, Path directory, long olderThan)
          throws IOException {
    ArrayList<LocatedFileStatus> fstats = new ArrayList<>();

    RemoteIterator<LocatedFileStatus> itr = fs.listFiles(directory, false);
    while( itr.hasNext() ) {
      LocatedFileStatus fileStatus = itr.next();
      if(olderThan>0 && fileStatus.getModificationTime()<olderThan )
        fstats.add(fileStatus);
      else
        fstats.add(fileStatus);
    }
    Collections.sort(fstats, new CmpFilesByModificationTime() );

    ArrayList<Path> result = new ArrayList<>(fstats.size());
    for (LocatedFileStatus fstat : fstats) {
      result.add(fstat.getPath());
    }
    return result;
  }

  public static class Pair<K,V> {
    private K key;
    private V value;
    public Pair(K key, V value) {
      this.key = key;
      this.value = value;
    }

    public K getKey() {
      return key;
    }

    public V getValue() {
      return value;
    }

    public static <K,V> Pair of(K key, V value) {
      return new Pair(key,value);
    }
  }  // class Pair
}
