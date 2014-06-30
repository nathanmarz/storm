package org.apache.storm.hbase.topology;


import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.storm.hbase.bolt.mapper.HBaseRowToStormValueMapper;

import java.util.ArrayList;
import java.util.List;

/**
 * Takes a Hbase result and returns a value list that has a value instance for each column and corresponding value.
 * So if the result from Hbase was
 * <pre>
 * WORD, COUNT
 * apple, 10
 * bannana, 20
 * </pre>
 *
 * this will return
 * <pre>
 *     [WORD, apple]
 *     [COUNT, 10]
 *     [WORD, banana]
 *     [COUNT, 20]
 * </pre>
 *
 */
public class WordCountRowToStormValueMapper implements HBaseRowToStormValueMapper {

    @Override
    public List<Values> toValues(Result result) throws Exception {
        List<Values> values = new ArrayList<Values>();
        Cell[] cells = result.rawCells();

        for(Cell cell : cells) {
            Values value = new Values (Bytes.toString(CellUtil.cloneQualifier(cell)), Bytes.toLong(CellUtil.cloneValue(cell)));
            values.add(value);
        }
        return values;
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("columnName","columnValue"));
    }

}
