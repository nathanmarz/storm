package org.apache.storm.jdbc.mapper;


import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.ITuple;
import backtype.storm.tuple.Values;
import org.apache.storm.jdbc.common.Column;

import java.util.ArrayList;
import java.util.List;

public class SimpleJdbcLookupMapper extends SimpleJdbcMapper implements JdbcLookupMapper {

    private Fields outputFields;

    public SimpleJdbcLookupMapper(Fields outputFields, List<Column> queryColumns) {
        super(queryColumns);
        this.outputFields = outputFields;
    }

    @Override
    public List<Values> toTuple(ITuple input, List<Column> columns) {
        Values values = new Values();

        for(String field : outputFields) {
            if(input.contains(field)) {
                values.add(input.getValueByField(field));
            } else {
                for(Column column : columns) {
                    if(column.getColumnName().equalsIgnoreCase(field)) {
                        values.add(column.getVal());
                    }
                }
            }
        }
        List<Values> result = new ArrayList<Values>();
        result.add(values);
        return result;
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(outputFields);
    }
}
