package org.apache.storm.flux.test;

import backtype.storm.Config;
import backtype.storm.generated.StormTopology;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import storm.kafka.StringScheme;
import storm.trident.TridentTopology;
import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.operation.builtin.Count;
import storm.trident.testing.FixedBatchSpout;
import storm.trident.testing.MemoryMapState;
import storm.trident.tuple.TridentTuple;

/**
 * Basic Trident example that will return a `StormTopology` from a `getTopology()` method.
 */
public class TridentTopologySource {

    private FixedBatchSpout spout;

    public StormTopology getTopology(Config config) {

        this.spout = new FixedBatchSpout(new Fields("sentence"), 20,
                new Values("one two"),
                new Values("two three"),
                new Values("three four"),
                new Values("four five"),
                new Values("five six")
        );


        TridentTopology trident = new TridentTopology();

        trident.newStream("wordcount", spout).name("sentence").parallelismHint(1).shuffle()
                .each(new Fields("sentence"), new Split(), new Fields("word"))
                .parallelismHint(1)
                .groupBy(new Fields("word"))
                .persistentAggregate(new MemoryMapState.Factory(), new Count(), new Fields("count"))
                .parallelismHint(1);
        return trident.build();
    }

    public static class Split extends BaseFunction {
        @Override
        public void execute(TridentTuple tuple, TridentCollector collector) {
            String sentence = tuple.getString(0);
            for (String word : sentence.split(" ")) {
                collector.emit(new Values(word));
            }
        }
    }
}
