package backtype.storm.drpc;

import backtype.storm.tuple.Fields;

public interface LinearDRPCInputDeclarer {
    public LinearDRPCInputDeclarer fieldsGrouping(Fields fields);
    public LinearDRPCInputDeclarer fieldsGrouping(int streamId, Fields fields);

    public LinearDRPCInputDeclarer globalGrouping();
    public LinearDRPCInputDeclarer globalGrouping(int streamId);

    public LinearDRPCInputDeclarer shuffleGrouping();
    public LinearDRPCInputDeclarer shuffleGrouping(int streamId);

    public LinearDRPCInputDeclarer noneGrouping();
    public LinearDRPCInputDeclarer noneGrouping(int streamId);

    public LinearDRPCInputDeclarer allGrouping();
    public LinearDRPCInputDeclarer allGrouping(int streamId);

    public LinearDRPCInputDeclarer directGrouping();
    public LinearDRPCInputDeclarer directGrouping(int streamId);    
}
