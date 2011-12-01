package backtype.storm.topology;

import backtype.storm.grouping.CustomStreamGrouping;
import backtype.storm.tuple.Fields;


public interface InputDeclarer {
    public InputDeclarer fieldsGrouping(String componentId, Fields fields);
    public InputDeclarer fieldsGrouping(String componentId, String streamId, Fields fields);

    public InputDeclarer globalGrouping(String componentId);
    public InputDeclarer globalGrouping(String componentId, String streamId);

    public InputDeclarer shuffleGrouping(String componentId);
    public InputDeclarer shuffleGrouping(String componentId, String streamId);

    public InputDeclarer noneGrouping(String componentId);
    public InputDeclarer noneGrouping(String componentId, String streamId);

    public InputDeclarer allGrouping(String componentId);
    public InputDeclarer allGrouping(String componentId, String streamId);

    public InputDeclarer directGrouping(String componentId);
    public InputDeclarer directGrouping(String componentId, String streamId);

    public InputDeclarer customGrouping(String componentId, CustomStreamGrouping grouping);
    public InputDeclarer customGrouping(String componentId, String streamId, CustomStreamGrouping grouping);

}
