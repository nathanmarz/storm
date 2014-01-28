package org.apache.storm.hdfs.bolt.format;

import backtype.storm.tuple.Tuple;
import org.apache.hadoop.io.Writable;

import java.io.Serializable;

public interface SequenceFormat extends Serializable {
    Class keyClass();
    Class valueClass();

    Writable key(Tuple tuple);
    Writable value(Tuple tuple);
}
