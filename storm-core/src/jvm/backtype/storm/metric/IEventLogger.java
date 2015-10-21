package backtype.storm.metric;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.Tuple;

import java.util.Date;
import java.util.Map;

/**
 * EventLogger interface for logging the event info to a sink like log file or db
 * for inspecting the events via UI for debugging.
 */
public interface IEventLogger {

    /**
     * A wrapper for the fields that we would log.
     */
    public static class EventInfo {
        String ts;
        String component;
        String task;
        String messageId;
        String values;
        EventInfo(String ts, String component, String task, String messageId, String values) {
            this.ts = ts;
            this.component = component;
            this.task = task;
            this.messageId = messageId;
            this.values = values;
        }

        /**
         * Returns a default formatted string with fields separated by ","
         *
         * @return a default formatted string with fields separated by ","
         */
        @Override
        public String toString() {
            return new StringBuilder(new Date(Long.parseLong(ts)).toString()).append(",")
                    .append(component).append(",")
                    .append(task).append(",")
                    .append(messageId).append(",")
                    .append(values).toString();
        }
    }

    void prepare(Map stormConf, TopologyContext context);

    /**
     * This method would be invoked when the {@link EventLoggerBolt} receives a tuple from the spouts or bolts that has
     * event logging enabled.
     *
     * @param e the event
     */
    void log(EventInfo e);

    void close();
}
