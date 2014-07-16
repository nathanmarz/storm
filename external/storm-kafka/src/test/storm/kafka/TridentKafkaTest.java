package storm.kafka;

import backtype.storm.Config;
import backtype.storm.tuple.Values;
import com.google.common.collect.Lists;
import kafka.javaapi.consumer.SimpleConsumer;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import storm.kafka.trident.TridentKafkaState;
import storm.trident.operation.TridentCollector;
import storm.trident.testing.MockTridentTuple;
import storm.trident.tuple.TridentTuple;

import java.util.ArrayList;
import java.util.List;

public class TridentKafkaTest {
    private KafkaTestBroker broker;
    private TridentKafkaState state;
    private Config config;
    private KafkaConfig kafkaConfig;
    private SimpleConsumer simpleConsumer;


    @Before
    public void setup() {
        broker = new KafkaTestBroker();
        simpleConsumer = TestUtils.getKafkaConsumer(broker);
        config = TestUtils.getConfig(broker.getBrokerConnectionString());
        config.put(TridentKafkaState.KEY_FIELD, "key");
        config.put(TridentKafkaState.MESSAGE_FIELD_NAME, "message");
        state = new TridentKafkaState();
        state.prepare(config);
    }

    @Test
    public void testKeyValue() {
        String keyString = "key-123";
        String valString = "message-123";
        int batchSize = 10;

        List<TridentTuple> tridentTuples = generateTupleBatch(keyString, valString, batchSize);

        state.updateState(tridentTuples, null);

        for(int i = 0 ; i < batchSize ; i++) {
            TestUtils.verifyMessage(keyString, valString, broker, simpleConsumer);
        }
    }

    private List<TridentTuple> generateTupleBatch(String key, String message, int batchsize) {
        List<TridentTuple> batch = new ArrayList<TridentTuple>();
        List<String> values = Lists.newArrayList(key, message);
        List<String> fields = Lists.newArrayList("key", "message");
        for(int i =0 ; i < batchsize; i++) {
            batch.add(new MockTridentTuple(fields, values));
        }
        return batch;
    }

    @After
    public void shutdown() {
        simpleConsumer.close();
        broker.shutdown();
    }
}
