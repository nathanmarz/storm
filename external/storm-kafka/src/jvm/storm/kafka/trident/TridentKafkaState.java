package storm.kafka.trident;


import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.FailedException;
import com.google.common.collect.Lists;
import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import storm.trident.operation.TridentCollector;
import storm.trident.state.State;
import storm.trident.tuple.TridentTuple;

import java.util.List;
import java.util.Map;
import java.util.Properties;

public class TridentKafkaState implements State {
    private static final Logger LOG = LoggerFactory.getLogger(TridentKafkaState.class);


    public static final String TOPIC = "topic";
    public static final String KAFKA_BROKER_PROPERTIES = "kafka.broker.properties";

    public static final String KEY_FIELD = "keyFieldName";
    public static final String MESSAGE_FIELD_NAME = "messageFieldName";

    private Producer producer;
    private OutputCollector collector;
    private String topic;
    private String keyField;
    private String msgField;

    @Override
    public void beginCommit(Long txid) {
        LOG.debug("beginCommit is Noop.");
    }

    @Override
    public void commit(Long txid) {
        LOG.debug("commit is Noop.");
    }

    public void prepare(Map stormConf) {
        Map configMap = (Map) stormConf.get(KAFKA_BROKER_PROPERTIES);
        Properties properties = new Properties();
        properties.putAll(configMap);
        ProducerConfig config = new ProducerConfig(properties);
        producer = new Producer(config);
        this.topic = (String) stormConf.get(TOPIC);
        this.keyField = (String) stormConf.get(KEY_FIELD);
        this.msgField = (String) stormConf.get(MESSAGE_FIELD_NAME);
    }

    public void updateState(List<TridentTuple> tuples, TridentCollector collector) {
        for (TridentTuple tuple : tuples) {
            try {
                if( tuple.getValueByField(keyField) != null) {
                    producer.send(new KeyedMessage(topic, tuple.getValueByField(keyField),
                            tuple.getValueByField(msgField)));
                }
            } catch (Exception ex) {
                String errorMsg = "Could not send message with key '" + tuple.getValueByField(keyField);
                LOG.warn(errorMsg, ex);
                throw new FailedException(errorMsg, ex);
            }
        }
    }
}
