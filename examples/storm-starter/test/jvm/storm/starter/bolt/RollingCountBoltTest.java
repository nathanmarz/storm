package storm.starter.bolt;

import backtype.storm.Config;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import org.testng.annotations.Test;
import storm.starter.tools.MockTupleHelpers;

import java.util.Map;

import static org.fest.assertions.api.Assertions.assertThat;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.*;

public class RollingCountBoltTest {

  private static final String ANY_NON_SYSTEM_COMPONENT_ID = "irrelevant_component_id";
  private static final String ANY_NON_SYSTEM_STREAM_ID = "irrelevant_stream_id";

  private Tuple mockNormalTuple(Object obj) {
    Tuple tuple = MockTupleHelpers.mockTuple(ANY_NON_SYSTEM_COMPONENT_ID, ANY_NON_SYSTEM_STREAM_ID);
    when(tuple.getValue(0)).thenReturn(obj);
    return tuple;
  }

  @SuppressWarnings("rawtypes")
  @Test
  public void shouldEmitNothingIfNoObjectHasBeenCountedYetAndTickTupleIsReceived() {
    // given
    Tuple tickTuple = MockTupleHelpers.mockTickTuple();
    RollingCountBolt bolt = new RollingCountBolt();
    Map conf = mock(Map.class);
    TopologyContext context = mock(TopologyContext.class);
    OutputCollector collector = mock(OutputCollector.class);
    bolt.prepare(conf, context, collector);

    // when
    bolt.execute(tickTuple);

    // then
    verifyZeroInteractions(collector);
  }

  @SuppressWarnings("rawtypes")
  @Test
  public void shouldEmitSomethingIfAtLeastOneObjectWasCountedAndTickTupleIsReceived() {
    // given
    Tuple normalTuple = mockNormalTuple(new Object());
    Tuple tickTuple = MockTupleHelpers.mockTickTuple();

    RollingCountBolt bolt = new RollingCountBolt();
    Map conf = mock(Map.class);
    TopologyContext context = mock(TopologyContext.class);
    OutputCollector collector = mock(OutputCollector.class);
    bolt.prepare(conf, context, collector);

    // when
    bolt.execute(normalTuple);
    bolt.execute(tickTuple);

    // then
    verify(collector).emit(any(Values.class));
  }

  @Test
  public void shouldDeclareOutputFields() {
    // given
    OutputFieldsDeclarer declarer = mock(OutputFieldsDeclarer.class);
    RollingCountBolt bolt = new RollingCountBolt();

    // when
    bolt.declareOutputFields(declarer);

    // then
    verify(declarer, times(1)).declare(any(Fields.class));

  }

  @Test
  public void shouldSetTickTupleFrequencyInComponentConfigurationToNonZeroValue() {
    // given
    RollingCountBolt bolt = new RollingCountBolt();

    // when
    Map<String, Object> componentConfig = bolt.getComponentConfiguration();

    // then
    assertThat(componentConfig).containsKey(Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS);
    Integer emitFrequencyInSeconds = (Integer) componentConfig.get(Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS);
    assertThat(emitFrequencyInSeconds).isGreaterThan(0);
  }
}
