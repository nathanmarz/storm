package org.apache.storm.hbase.trident.mapper;


import backtype.storm.tuple.Tuple;
import org.apache.storm.hbase.common.ColumnList;
import storm.trident.tuple.TridentTuple;

import java.io.Serializable;
/**
 * Maps a <code>storm.trident.tuple.TridentTuple</code> object
 * to a row in an HBase table.
 */
public interface TridentHBaseMapper extends Serializable {


        /**
         * Given a tuple, return the HBase rowkey.
         *
         * @param tuple
         * @return
         */
        byte[] rowKey(TridentTuple tuple);

        /**
         * Given a tuple, return a list of HBase columns to insert.
         *
         * @param tuple
         * @return
         */
        ColumnList columns(TridentTuple tuple);
}
