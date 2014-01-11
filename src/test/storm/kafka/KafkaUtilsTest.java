package storm.kafka;

import backtype.storm.utils.Utils;
import kafka.api.OffsetRequest;
import kafka.javaapi.consumer.SimpleConsumer;
import kafka.javaapi.message.ByteBufferMessageSet;
import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import storm.kafka.trident.GlobalPartitionInformation;

import java.util.Properties;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

public class KafkaUtilsTest {

    private KafkaTestBroker broker;
    private SimpleConsumer simpleConsumer;
    private KafkaConfig config;

    @Before
    public void setup() {
        broker = new KafkaTestBroker();
        GlobalPartitionInformation globalPartitionInformation = new GlobalPartitionInformation();
        globalPartitionInformation.addPartition(0, Broker.fromString(broker.getBrokerConnectionString()));
        BrokerHosts brokerHosts = new StaticHosts(globalPartitionInformation);
        config = new KafkaConfig(brokerHosts, "testTopic");
        simpleConsumer = new SimpleConsumer("localhost", broker.getPort(), 60000, 1024, "testClient");
    }

    @After
    public void shutdown() {
        broker.shutdown();
    }


    @Test(expected = FailedFetchException.class)
    public void topicDoesNotExist() throws Exception {
        KafkaUtils.fetchMessages(config, simpleConsumer, new Partition(Broker.fromString(broker.getBrokerConnectionString()), 0), 0);
    }

    @Test(expected = FailedFetchException.class)
    public void brokerIsDown() throws Exception {
        int port = broker.getPort();
        broker.shutdown();
        SimpleConsumer simpleConsumer = new SimpleConsumer("localhost", port, 100, 1024, "testClient");
        KafkaUtils.fetchMessages(config, simpleConsumer, new Partition(Broker.fromString(broker.getBrokerConnectionString()), 0), OffsetRequest.LatestTime());
    }

    @Test
    public void fetchMessage() throws Exception {
        long lastOffset = KafkaUtils.getOffset(simpleConsumer, config.topic, 0, OffsetRequest.EarliestTime());
        sendMessageAndAssertValueForOffset(lastOffset);
    }

    @Test(expected = FailedFetchException.class)
    public void fetchMessagesWithInvalidOffsetAndDefaultHandlingDisabled() throws Exception {
        config.useStartOffsetTimeIfOffsetOutOfRange = false;
        sendMessageAndAssertValueForOffset(-99);
    }

    @Test
    public void fetchMessagesWithInvalidOffsetAndDefaultHandlingEnabled() throws Exception {
        sendMessageAndAssertValueForOffset(-99);
    }

    private String createTopicAndSendMessage() {
        Properties p = new Properties();
        p.setProperty("metadata.broker.list", "localhost:49123");
        p.setProperty("serializer.class", "kafka.serializer.StringEncoder");
        ProducerConfig producerConfig = new ProducerConfig(p);
        Producer<String, String> producer = new Producer<String, String>(producerConfig);
        String value = "value";
        producer.send(new KeyedMessage<String, String>(config.topic, value));
        return value;
    }

    private void sendMessageAndAssertValueForOffset(long offset) {
        String value = createTopicAndSendMessage();
        ByteBufferMessageSet messageAndOffsets = KafkaUtils.fetchMessages(config, simpleConsumer, new Partition(Broker.fromString(broker.getBrokerConnectionString()), 0), offset);
        String message = new String(Utils.toByteArray(messageAndOffsets.iterator().next().message().payload()));
        assertThat(message, is(equalTo(value)));
    }
}
