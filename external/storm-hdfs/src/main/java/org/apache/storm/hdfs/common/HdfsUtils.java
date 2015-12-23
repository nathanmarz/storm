/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.storm.hdfs.common;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileAlreadyExistsException;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.protocol.AlreadyBeingCreatedException;
import org.apache.hadoop.ipc.RemoteException;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;

public class HdfsUtils {
  /** list files sorted by modification time that have not been modified since 'olderThan'. if
   * 'olderThan' is <= 0 then the filtering is disabled */
  public static ArrayList<Path> listFilesByModificationTime(FileSystem fs, Path directory, long olderThan)
          throws IOException {
    ArrayList<LocatedFileStatus> fstats = new ArrayList<>();

    RemoteIterator<LocatedFileStatus> itr = fs.listFiles(directory, false);
    while( itr.hasNext() ) {
      LocatedFileStatus fileStatus = itr.next();
      if(olderThan>0) {
        if( fileStatus.getModificationTime()<=olderThan )
          fstats.add(fileStatus);
      }
      else {
        fstats.add(fileStatus);
      }
    }
    Collections.sort(fstats, new ModifTimeComparator() );

    ArrayList<Path> result = new ArrayList<>(fstats.size());
    for (LocatedFileStatus fstat : fstats) {
      result.add(fstat.getPath());
    }
    return result;
  }

  /**
   * Returns null if file already exists. throws if there was unexpected problem
   */
  public static FSDataOutputStream tryCreateFile(FileSystem fs, Path file) throws IOException {
    try {
      FSDataOutputStream os = fs.create(file, false);
      return os;
    } catch (FileAlreadyExistsException e) {
      return null;
    } catch (RemoteException e) {
      if( e.unwrapRemoteException() instanceof AlreadyBeingCreatedException ) {
        return null;
      } else { // unexpected error
        throw e;
      }
    }
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
