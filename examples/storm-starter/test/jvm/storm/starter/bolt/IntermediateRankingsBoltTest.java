package storm.starter.bolt;

import backtype.storm.Config;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import com.google.common.collect.Lists;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;
import storm.starter.tools.MockTupleHelpers;

import java.util.Map;

import static org.fest.assertions.api.Assertions.assertThat;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.*;

public class IntermediateRankingsBoltTest {

  private static final String ANY_NON_SYSTEM_COMPONENT_ID = "irrelevant_component_id";
  private static final String ANY_NON_SYSTEM_STREAM_ID = "irrelevant_stream_id";
  private static final Object ANY_OBJECT = new Object();
  private static final int ANY_TOPN = 10;
  private static final long ANY_COUNT = 42;

  private Tuple mockRankableTuple(Object obj, long count) {
    Tuple tuple = MockTupleHelpers.mockTuple(ANY_NON_SYSTEM_COMPONENT_ID, ANY_NON_SYSTEM_STREAM_ID);
    when(tuple.getValues()).thenReturn(Lists.newArrayList(ANY_OBJECT, ANY_COUNT));
    return tuple;
  }

  @DataProvider
  public Object[][] illegalTopN() {
    return new Object[][]{ { -10 }, { -3 }, { -2 }, { -1 }, { 0 } };
  }

  @Test(expectedExceptions = IllegalArgumentException.class, dataProvider = "illegalTopN")
  public void negativeOrZeroTopNShouldThrowIAE(int topN) {
    new IntermediateRankingsBolt(topN);
  }

  @DataProvider
  public Object[][] illegalEmitFrequency() {
    return new Object[][]{ { -10 }, { -3 }, { -2 }, { -1 }, { 0 } };
  }

  @Test(expectedExceptions = IllegalArgumentException.class, dataProvider = "illegalEmitFrequency")
  public void negativeOrZeroEmitFrequencyShouldThrowIAE(int emitFrequencyInSeconds) {
    new IntermediateRankingsBolt(ANY_TOPN, emitFrequencyInSeconds);
  }

  @DataProvider
  public Object[][] legalTopN() {
    return new Object[][]{ { 1 }, { 2 }, { 3 }, { 20 } };
  }

  @Test(dataProvider = "legalTopN")
  public void positiveTopNShouldBeOk(int topN) {
    new IntermediateRankingsBolt(topN);
  }

  @DataProvider
  public Object[][] legalEmitFrequency() {
    return new Object[][]{ { 1 }, { 2 }, { 3 }, { 20 } };
  }

  @Test(dataProvider = "legalEmitFrequency")
  public void positiveEmitFrequencyShouldBeOk(int emitFrequencyInSeconds) {
    new IntermediateRankingsBolt(ANY_TOPN, emitFrequencyInSeconds);
  }

  @Test
  public void shouldEmitSomethingIfTickTupleIsReceived() {
    // given
    Tuple tickTuple = MockTupleHelpers.mockTickTuple();
    BasicOutputCollector collector = mock(BasicOutputCollector.class);
    IntermediateRankingsBolt bolt = new IntermediateRankingsBolt();

    // when
    bolt.execute(tickTuple, collector);

    // then
    // verifyZeroInteractions(collector);
    verify(collector).emit(any(Values.class));
  }

  @Test
  public void shouldEmitNothingIfNormalTupleIsReceived() {
    // given
    Tuple normalTuple = mockRankableTuple(ANY_OBJECT, ANY_COUNT);
    BasicOutputCollector collector = mock(BasicOutputCollector.class);
    IntermediateRankingsBolt bolt = new IntermediateRankingsBolt();

    // when
    bolt.execute(normalTuple, collector);

    // then
    verifyZeroInteractions(collector);
  }

  @Test
  public void shouldDeclareOutputFields() {
    // given
    OutputFieldsDeclarer declarer = mock(OutputFieldsDeclarer.class);
    IntermediateRankingsBolt bolt = new IntermediateRankingsBolt();

    // when
    bolt.declareOutputFields(declarer);

    // then
    verify(declarer, times(1)).declare(any(Fields.class));
  }

  @Test
  public void shouldSetTickTupleFrequencyInComponentConfigurationToNonZeroValue() {
    // given
    IntermediateRankingsBolt bolt = new IntermediateRankingsBolt();

    // when
    Map<String, Object> componentConfig = bolt.getComponentConfiguration();

    // then
    assertThat(componentConfig).containsKey(Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS);
    Integer emitFrequencyInSeconds = (Integer) componentConfig.get(Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS);
    assertThat(emitFrequencyInSeconds).isGreaterThan(0);
  }
}
