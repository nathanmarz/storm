package backtype.storm.metric;

import backtype.storm.task.IBolt;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.Map;
import static backtype.storm.metric.IEventLogger.EventInfo;

public class EventLoggerBolt implements IBolt {

    private static final Logger LOG = LoggerFactory.getLogger(EventLoggerBolt.class);

    /*
     The below field declarations are also used in common.clj to define the event logger output fields
      */
    public static final String FIELD_TS = "ts";
    public static final String FIELD_VALUES = "values";
    public static final String FIELD_COMPONENT_ID = "component-id";
    public static final String FIELD_MESSAGE_ID = "message-id";

    private IEventLogger eventLogger;

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        LOG.info("EventLoggerBolt prepare called");
        eventLogger = new FileBasedEventLogger();
        eventLogger.prepare(stormConf, context);
    }

    @Override
    public void execute(Tuple input) {
        LOG.debug("** EventLoggerBolt got tuple from sourceComponent {}, with values {}", input.getSourceComponent(), input.getValues());

        Object msgId = input.getValueByField(FIELD_MESSAGE_ID);
        EventInfo eventInfo = new EventInfo(input.getValueByField(FIELD_TS).toString(), input.getSourceComponent(),
                                            String.valueOf(input.getSourceTask()), msgId == null ? "" : msgId.toString(),
                                            input.getValueByField(FIELD_VALUES).toString());

        eventLogger.log(eventInfo);
    }

    @Override
    public void cleanup() {
        eventLogger.close();
    }
}