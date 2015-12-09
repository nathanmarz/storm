package org.apache.storm.hdfs.common;

import org.apache.hadoop.fs.LocatedFileStatus;

import java.util.Comparator;


public class CmpFilesByModificationTime
        implements Comparator<LocatedFileStatus> {
   @Override
    public int compare(LocatedFileStatus o1, LocatedFileStatus o2) {
      return new Long(o1.getModificationTime()).compareTo( o1.getModificationTime() );
    }
}
