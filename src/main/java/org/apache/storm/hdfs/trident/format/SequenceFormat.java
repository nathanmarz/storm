package org.apache.storm.hdfs.trident.format;

import org.apache.hadoop.io.Writable;
import storm.trident.tuple.TridentTuple;

import java.io.Serializable;

/**
 * Interface for converting <code>TridentTuple</code> objects to HDFS sequence file key-value pairs.
 *
 */
public interface SequenceFormat extends Serializable {
    /**
     * Key class used by implementation (e.g. IntWritable.class, etc.)
     *
     * @return
     */
    Class keyClass();

    /**
     * Value class used by implementation (e.g. Text.class, etc.)
     * @return
     */
    Class valueClass();

    /**
     * Given a tuple, return the key that should be written to the sequence file.
     *
     * @param tuple
     * @return
     */
    Writable key(TridentTuple tuple);

    /**
     * Given a tuple, return the value that should be written to the sequence file.
     * @param tuple
     * @return
     */
    Writable value(TridentTuple tuple);
}
