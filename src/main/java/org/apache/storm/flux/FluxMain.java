package org.apache.storm.flux;


import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.spout.ISpout;
import backtype.storm.topology.*;
import backtype.storm.tuple.Fields;
import backtype.storm.utils.Utils;
import org.apache.storm.flux.model.*;
import org.apache.storm.flux.parser.FluxParser;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class FluxMain {
    public static void main(String[] args) throws Exception {


        TopologyDef topologyDef = FluxParser.parse("src/test/resources/configs/tck.yaml");

        TopologyBuilder builder = new TopologyBuilder();



        // create spouts
        for(SpoutDef sd : topologyDef.getSpouts()){
            builder.setSpout(sd.getId(), buildSpout(sd), sd.getParallelism());
        }

        // we need to be able to lookup bolts by id, then switch based
        // on whether they are IBasicBolt or IRichBolt instances
        Map<String, Object> boltMap = buildBoltMap(topologyDef.getBolts());

        for(StreamDef stream : topologyDef.getStreams()){
            Object boltObj = boltMap.get(stream.getTo());
            BoltDeclarer declarer = null;
            if(boltObj instanceof  IRichBolt) {
                declarer = builder.setBolt(stream.getTo(), (IRichBolt) boltObj, topologyDef.parallelismForBolt(stream.getTo()));
            } else if (boltObj instanceof IBasicBolt){
                declarer = builder.setBolt(stream.getTo(), (IBasicBolt) boltObj, topologyDef.parallelismForBolt(stream.getTo()));
            } else {
                throw new IllegalArgumentException("Class does not appear to be a bolt: " + boltObj.getClass().getName());
            }

            GroupingDef grouping = stream.getGrouping();
            // if the streamId is defined, use it for the grouping, otherwise assume storm's default stream
            String streamId = (grouping.getStreamId() == null ? Utils.DEFAULT_STREAM_ID : grouping.getStreamId());

            // todo make grouping types an enum
            if(grouping.getType().equals("shuffle")){
                declarer.shuffleGrouping(stream.getFrom(), streamId);
            } else if(grouping.getType().equals("fields")){
                declarer.fieldsGrouping(stream.getFrom(), streamId, new Fields(grouping.getArgs()));
            } else {
                throw new UnsupportedOperationException("unsupported grouping type: " + grouping.getStreamId());
            }
        }

        builder.createTopology();

        Config conf = new Config();
        conf.putAll(topologyDef.getConfig());

        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("test", conf, builder.createTopology());
        Utils.sleep(60000);
        cluster.killTopology("test");
        cluster.shutdown();

    }



    public static IRichSpout buildSpout(SpoutDef def) throws ClassNotFoundException, IllegalAccessException, InstantiationException {
        Class clazz = Class.forName(def.getClassName());

        //TODO Deal with constructor args
        IRichSpout spout = (IRichSpout)clazz.newInstance();


        return spout;
    }

    public static Map<String, Object> buildBoltMap(List<BoltDef> boltDefs) throws ClassNotFoundException, IllegalAccessException, InstantiationException {
        Map<String, Object> retval= new HashMap<String, Object>();
        for(BoltDef bd : boltDefs){
            Class clazz = Class.forName(bd.getClassName());
            // TODO deal with constructor args
            retval.put(bd.getId(), clazz.newInstance());
        }

        return retval;
    }
}

