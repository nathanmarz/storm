package org.apache.storm.hdfs.bolt.format;

import backtype.storm.tuple.Tuple;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

/**
 * Basic <code>SequenceFormat</code> implementation that uses
 * <code>LongWritable</code> for keys and <code>Text</code> for values.
 *
 */
public class DefaultSequenceFormat implements SequenceFormat {
    private transient LongWritable key;
    private transient Text value;

    private String keyField;
    private String valueField;

    public DefaultSequenceFormat(String keyField, String valueField){
        this.keyField = keyField;
        this.valueField = valueField;
    }

    @Override
    public Class keyClass() {
        return LongWritable.class;
    }

    @Override
    public Class valueClass() {
        return Text.class;
    }

    @Override
    public Writable key(Tuple tuple) {
        if(this.key == null){
            this.key  = new LongWritable();
        }
        this.key.set(tuple.getLongByField(this.keyField));
        return this.key;
    }

    @Override
    public Writable value(Tuple tuple) {
        if(this.value == null){
            this.value = new Text();
        }
        this.value.set(tuple.getStringByField(this.valueField));
        return this.value;
    }
}
