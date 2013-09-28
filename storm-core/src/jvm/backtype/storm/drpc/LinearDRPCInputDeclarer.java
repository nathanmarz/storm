package backtype.storm.drpc;

import backtype.storm.grouping.CustomStreamGrouping;
import backtype.storm.topology.ComponentConfigurationDeclarer;
import backtype.storm.tuple.Fields;

public interface LinearDRPCInputDeclarer extends ComponentConfigurationDeclarer<LinearDRPCInputDeclarer> {
    public LinearDRPCInputDeclarer fieldsGrouping(Fields fields);
    public LinearDRPCInputDeclarer fieldsGrouping(String streamId, Fields fields);

    public LinearDRPCInputDeclarer globalGrouping();
    public LinearDRPCInputDeclarer globalGrouping(String streamId);

    public LinearDRPCInputDeclarer shuffleGrouping();
    public LinearDRPCInputDeclarer shuffleGrouping(String streamId);

    public LinearDRPCInputDeclarer localOrShuffleGrouping();
    public LinearDRPCInputDeclarer localOrShuffleGrouping(String streamId);
    
    public LinearDRPCInputDeclarer noneGrouping();
    public LinearDRPCInputDeclarer noneGrouping(String streamId);

    public LinearDRPCInputDeclarer allGrouping();
    public LinearDRPCInputDeclarer allGrouping(String streamId);

    public LinearDRPCInputDeclarer directGrouping();
    public LinearDRPCInputDeclarer directGrouping(String streamId);
    
    public LinearDRPCInputDeclarer customGrouping(CustomStreamGrouping grouping);
    public LinearDRPCInputDeclarer customGrouping(String streamId, CustomStreamGrouping grouping);
    
}
