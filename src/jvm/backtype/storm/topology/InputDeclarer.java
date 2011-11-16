package backtype.storm.topology;

import backtype.storm.grouping.CustomStreamGrouping;
import backtype.storm.tuple.Fields;


public interface InputDeclarer {
    public InputDeclarer fieldsGrouping(int componentId, Fields fields);
    public InputDeclarer fieldsGrouping(int componentId, int streamId, Fields fields);

    public InputDeclarer globalGrouping(int componentId);
    public InputDeclarer globalGrouping(int componentId, int streamId);

    public InputDeclarer shuffleGrouping(int componentId);
    public InputDeclarer shuffleGrouping(int componentId, int streamId);

    public InputDeclarer noneGrouping(int componentId);
    public InputDeclarer noneGrouping(int componentId, int streamId);

    public InputDeclarer allGrouping(int componentId);
    public InputDeclarer allGrouping(int componentId, int streamId);

    public InputDeclarer directGrouping(int componentId);
    public InputDeclarer directGrouping(int componentId, int streamId);

    public InputDeclarer customGrouping(int componentId, CustomStreamGrouping grouping);
    public InputDeclarer customGrouping(int componentId, int streamId, CustomStreamGrouping grouping);

}
