package org.apache.storm.jdbc.mapper;

import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.ITuple;
import backtype.storm.tuple.Values;
import org.apache.storm.jdbc.common.Column;

import java.util.List;

public interface JdbcLookupMapper extends JdbcMapper {

    /**
     * Covers a DB row to a list of storm values that can be emitted. This is done to allow a single
     * storm input tuple and a single DB row to result in multiple output values.
     * @param input the input tuple.
     * @param columns list of columns that represents a row
     * @return a List of storm values that can be emitted. Each item in list is emitted as an output tuple.
     */
    public List<Values> toTuple(ITuple input, List<Column> columns);

    /**
     * declare what are the fields that this code will output.
     * @param declarer
     */
    void declareOutputFields(OutputFieldsDeclarer declarer);
}
