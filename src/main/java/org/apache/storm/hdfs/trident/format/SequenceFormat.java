package org.apache.storm.hdfs.trident.format;

import org.apache.hadoop.io.Writable;
import storm.trident.tuple.TridentTuple;

import java.io.Serializable;

public interface SequenceFormat extends Serializable {
    Class keyClass();
    Class valueClass();

    Writable key(TridentTuple tuple);
    Writable value(TridentTuple tuple);
}
