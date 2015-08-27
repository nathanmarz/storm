package storm.trident;

import backtype.storm.generated.Bolt;
import backtype.storm.generated.StormTopology;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import org.junit.Assert;
import org.junit.Test;
import storm.trident.operation.builtin.Count;
import storm.trident.operation.builtin.FilterNull;
import storm.trident.operation.builtin.Sum;
import storm.trident.operation.impl.FilterExecutor;
import storm.trident.testing.FixedBatchSpout;
import storm.trident.testing.Split;
import storm.trident.testing.StringLength;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * Created by jiasheng on 15-8-25.
 */
public class TestTridentTopology {

    private StormTopology buildTopology() {
        FixedBatchSpout spout = new FixedBatchSpout(new Fields("sentence"), 3,
                new Values("the cow jumped over the moon"),
                new Values("the man went to the store and bought some candy"),
                new Values("four score and seven years ago"),
                new Values("how many apples can you eat"));
        spout.setCycle(true);

        TridentTopology topology = new TridentTopology();
        topology.newStream("spout", spout)
                //no name
                .each(new Fields("sentence"), new Split(), new Fields("word"))
                .partitionBy(new Fields("word"))
                .name("abc")
                .each(new Fields("word"), new StringLength(), new Fields("length"))
                .partitionBy(new Fields("length"))
                .name("def")
                .aggregate(new Fields("length"), new Count(), new Fields("count"))
                .partitionBy(new Fields("count"))
                .name("ghi")
                .aggregate(new Fields("count"), new Sum(), new Fields("sum"));
        return topology.build();
    }

    @Test
    public void testGenBoltId() {
        Set<String> pre = null;
        for (int i = 0; i < 100; i++) {
            StormTopology topology = buildTopology();
            Map<String, Bolt> cur = topology.get_bolts();
            System.out.println(cur.keySet());
            if (pre != null) {
                Assert.assertTrue("bold id not consistent with group name", pre.equals(cur.keySet()));
            }
            pre = cur.keySet();
        }
    }

}
