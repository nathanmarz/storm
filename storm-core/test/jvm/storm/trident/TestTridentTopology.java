/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.storm.trident;

import org.apache.storm.generated.Bolt;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.junit.Assert;
import org.junit.Test;
import org.apache.storm.trident.operation.builtin.Count;
import org.apache.storm.trident.operation.builtin.Sum;
import org.apache.storm.trident.testing.FixedBatchSpout;
import org.apache.storm.trident.testing.Split;
import org.apache.storm.trident.testing.StringLength;

import java.util.Map;
import java.util.Set;

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
