package org.apache.storm.flux.spouts;

import backtype.storm.spout.ShellSpout;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import java.util.Map;

/**
 * A generic `ShellSpout` implementation that allows you specify output fields
 * without having to subclass `ShellSpout` to do so.
 *
 */
public class GenericShellSpout extends ShellSpout implements IRichSpout {
    private String[] outputFields;

    /**
     * Create a ShellSpout with command line arguments and output fields
     * @param args Command line arguments for the spout
     * @param outputFields Names of fields the spout will emit.
     */
    public GenericShellSpout(String[] args, String[] outputFields){
        super(args);
        this.outputFields = outputFields;
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields(this.outputFields));
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
}
