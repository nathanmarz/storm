package org.apache.storm.flux.bolts;

import backtype.storm.task.ShellBolt;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;

import java.util.Map;

/**
 * A generic `ShellBolt` implementation that allows you specify output fields
 * without having to subclass `ShellBolt` to do so.
 *
 */
public class GenericShellBolt extends ShellBolt implements IRichBolt{
    private String[] outputFields;

    /**
     * Create a ShellBolt with command line arguments and output fields
     * @param command Command line arguments for the bolt
     * @param outputFields Names of fields the bolt will emit (if any).
     */

    public GenericShellBolt(String[] command, String[] outputFields){
        super(command);
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
