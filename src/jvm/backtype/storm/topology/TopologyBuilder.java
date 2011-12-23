package backtype.storm.topology;

import backtype.storm.generated.Bolt;
import backtype.storm.generated.ComponentCommon;
import backtype.storm.generated.ComponentObject;
import backtype.storm.generated.GlobalStreamId;
import backtype.storm.generated.Grouping;
import backtype.storm.generated.NullStruct;
import backtype.storm.generated.SpoutSpec;
import backtype.storm.generated.StateSpoutSpec;
import backtype.storm.generated.StormTopology;
import backtype.storm.generated.TransactionalBolt;
import backtype.storm.generated.TransactionalSpoutSpec;
import backtype.storm.grouping.CustomStreamGrouping;
import backtype.storm.tuple.Fields;
import backtype.storm.utils.Utils;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

/**
 * TopologyBuilder exposes the Java API for specifying a topology for Storm
 * to execute. Topologies are Thrift structures in the end, but since the Thrift API
 * is so verbose, TopologyBuilder greatly eases the process of creating topologies.
 * The template for creating and submitting a topology looks something like:
 *
 * <pre>
 * TopologyBuilder builder = new TopologyBuilder();
 *
 * builder.setSpout("1", new TestWordSpout(true), 5);
 * builder.setSpout("2", new TestWordSpout(true), 3);
 * builder.setBolt("3", new TestWordCounter(), 3)
 *          .fieldsGrouping("1", new Fields("word"))
 *          .fieldsGrouping("2", new Fields("word"));
 * builder.setBolt("4", new TestGlobalCount())
 *          .globalGrouping("1");
 *
 * Map conf = new HashMap();
 * conf.put(Config.TOPOLOGY_WORKERS, 4);
 * 
 * StormSubmitter.submitTopology("mytopology", conf, builder.createTopology());
 * </pre>
 *
 * Running the exact same topology in local mode (in process), and configuring it to log all tuples
 * emitted, looks like the following. Note that it lets the topology run for 10 seconds
 * before shutting down the local cluster.
 *
 * <pre>
 * TopologyBuilder builder = new TopologyBuilder();
 *
 * builder.setSpout("1", new TestWordSpout(true), 5);
 * builder.setSpout("2", new TestWordSpout(true), 3);
 * builder.setBolt("3", new TestWordCounter(), 3)
 *          .fieldsGrouping("1", new Fields("word"))
 *          .fieldsGrouping("2", new Fields("word"));
 * builder.setBolt("4", new TestGlobalCount())
 *          .globalGrouping("1");
 *
 * Map conf = new HashMap();
 * conf.put(Config.TOPOLOGY_WORKERS, 4);
 * conf.put(Config.TOPOLOGY_DEBUG, true);
 *
 * LocalCluster cluster = new LocalCluster();
 * cluster.submitTopology("mytopology", conf, builder.createTopology());
 * Utils.sleep(10000);
 * cluster.shutdown();
 * </pre>
 *
 * <p>The pattern for TopologyBuilder is to map component ids to components using the setSpout
 * and setBolt methods. Those methods return objects that are then used to declare
 * the inputs for that component.</p>
 */
public class TopologyBuilder {
    private Map<String, IRichBolt> _bolts = new HashMap<String, IRichBolt>();
    private Map<String, Map<GlobalStreamId, Grouping>> _inputs = new HashMap<String, Map<GlobalStreamId, Grouping>>();
    private Map<String, SpoutSpec> _spouts = new HashMap<String, SpoutSpec>();
    private Map<String, StateSpoutSpec> _stateSpouts = new HashMap<String, StateSpoutSpec>();
    private Map<String, Integer> _boltParallelismHints = new HashMap<String, Integer>();
        
    public StormTopology createTopology() {
        Map<String, Bolt> boltSpecs = new HashMap<String, Bolt>();
        for(String boltId: _bolts.keySet()) {
            IRichBolt bolt = _bolts.get(boltId);
            Integer parallelism_hint = _boltParallelismHints.get(boltId);
            Map<GlobalStreamId, Grouping> inputs = _inputs.get(boltId);
            ComponentCommon common = getComponentCommon(inputs, bolt, parallelism_hint);
            if(parallelism_hint!=null) {
                common.set_parallelism_hint(parallelism_hint);
            }
            boltSpecs.put(boltId, new Bolt(ComponentObject.serialized_java(Utils.serialize(bolt)), common));
        }
        return new StormTopology(new HashMap<String, SpoutSpec>(_spouts),
                                 boltSpecs,
                                 new HashMap<String, StateSpoutSpec>(_stateSpouts),
                                 new HashMap<String, TransactionalSpoutSpec>(),
                                 new HashMap<String, TransactionalBolt>());
    }

    /**
     * Define a new bolt in this topology with parallelism of just one thread.
     *
     * @param id the id of this component. This id is referenced by other components that want to consume this bolt's outputs.
     * @param bolt the bolt
     * @return use the returned object to declare the inputs to this component
     */
    public InputDeclarer setBolt(String id, IRichBolt bolt) {
        return setBolt(id, bolt, null);
    }

    /**
     * Define a new bolt in this topology with the specified amount of parallelism.
     *
     * @param id the id of this component. This id is referenced by other components that want to consume this bolt's outputs.
     * @param bolt the bolt
     * @param parallelism_hint the number of tasks that should be assigned to execute this bolt. Each task will run on a thread in a process somewhere around the cluster.
     * @return use the returned object to declare the inputs to this component
     */
    public InputDeclarer setBolt(String id, IRichBolt bolt, Integer parallelism_hint) {
        validateUnusedId(id);
        _bolts.put(id, bolt);
        _boltParallelismHints.put(id, parallelism_hint);
        _inputs.put(id, new HashMap<GlobalStreamId, Grouping>());
        return new InputGetter(id);
    }

    /**
     * Define a new bolt in this topology. This defines a basic bolt, which is a
     * simpler to use but more restricted kind of bolt. Basic bolts are intended
     * for non-aggregation processing and automate the anchoring/acking process to
     * achieve proper reliability in the topology.
     *
     * @param id the id of this component. This id is referenced by other components that want to consume this bolt's outputs.
     * @param bolt the basic bolt
     * @return use the returned object to declare the inputs to this component
     */
    public InputDeclarer setBolt(String id, IBasicBolt bolt) {
        return setBolt(id, bolt, null);
    }

    /**
     * Define a new bolt in this topology. This defines a basic bolt, which is a
     * simpler to use but more restricted kind of bolt. Basic bolts are intended
     * for non-aggregation processing and automate the anchoring/acking process to
     * achieve proper reliability in the topology.
     *
     * @param id the id of this component. This id is referenced by other components that want to consume this bolt's outputs.
     * @param bolt the basic bolt
     * @param parallelism_hint the number of tasks that should be assigned to execute this bolt. Each task will run on a thread in a process somwehere around the cluster.
     * @return use the returned object to declare the inputs to this component
     */
    public InputDeclarer setBolt(String id, IBasicBolt bolt, Integer parallelism_hint) {
        return setBolt(id, new BasicBoltExecutor(bolt), parallelism_hint);
    }

    /**
     * Define a new spout in this topology.
     *
     * @param id the id of this component. This id is referenced by other components that want to consume this spout's outputs.
     * @param spout the spout
     */
    public void setSpout(String id, IRichSpout spout) {
        setSpout(id, spout, null);
    }

    /**
     * Define a new spout in this topology with the specified parallelism. If the spout declares
     * itself as non-distributed, the parallelism_hint will be ignored and only one task
     * will be allocated to this component.
     *
     * @param id the id of this component. This id is referenced by other components that want to consume this spout's outputs.
     * @param parallelism_hint the number of tasks that should be assigned to execute this spout. Each task will run on a thread in a process somwehere around the cluster.
     * @param spout the spout
     */
    public void setSpout(String id, IRichSpout spout, Integer parallelism_hint) {
        validateUnusedId(id);
        _spouts.put(id, new SpoutSpec(ComponentObject.serialized_java(Utils.serialize(spout)), getComponentCommon(null, spout, parallelism_hint), spout.isDistributed()));
    }

    public void setStateSpout(String id, IRichStateSpout stateSpout) {
        setStateSpout(id, stateSpout, null);
    }

    public void setStateSpout(String id, IRichStateSpout stateSpout, Integer parallelism_hint) {
        validateUnusedId(id);
        _stateSpouts.put(id,
                         new StateSpoutSpec(
                             ComponentObject.serialized_java(Utils.serialize(stateSpout)),
                             getComponentCommon(null, stateSpout, parallelism_hint)));
    }


    private void validateUnusedId(String id) {
        if(_bolts.containsKey(id)) {
            throw new IllegalArgumentException("Bolt has already been declared for id " + id);
        }
        if(_spouts.containsKey(id)) {
            throw new IllegalArgumentException("Spout has already been declared for id " + id);
        }
        if(_stateSpouts.containsKey(id)) {
            throw new IllegalArgumentException("State spout has already been declared for id " + id);
        }
    }

    private ComponentCommon getComponentCommon(Map<GlobalStreamId, Grouping> inputs, IComponent component, Integer parallelism_hint) {
        if(inputs==null) inputs = new HashMap<GlobalStreamId, Grouping>();
        OutputFieldsGetter getter = new OutputFieldsGetter();
        component.declareOutputFields(getter);
        ComponentCommon common = new ComponentCommon(inputs, getter.getFieldsDeclaration());
        if(parallelism_hint!=null) {
            common.set_parallelism_hint(parallelism_hint);
        }
        return common;
        
    }

    protected class InputGetter implements InputDeclarer {
        private String _boltId;

        public InputGetter(String boltId) {
            _boltId = boltId;
        }

        public InputDeclarer fieldsGrouping(String componentId, Fields fields) {
            return fieldsGrouping(componentId, Utils.DEFAULT_STREAM_ID, fields);
        }

        public InputDeclarer fieldsGrouping(String componentId, String streamId, Fields fields) {
            return grouping(componentId, streamId, Grouping.fields(fields.toList()));
        }

        public InputDeclarer globalGrouping(String componentId) {
            return globalGrouping(componentId, Utils.DEFAULT_STREAM_ID);
        }

        public InputDeclarer globalGrouping(String componentId, String streamId) {
            return grouping(componentId, streamId, Grouping.fields(new ArrayList<String>()));
        }

        public InputDeclarer shuffleGrouping(String componentId) {
            return shuffleGrouping(componentId, Utils.DEFAULT_STREAM_ID);
        }

        public InputDeclarer shuffleGrouping(String componentId, String streamId) {
            return grouping(componentId, streamId, Grouping.shuffle(new NullStruct()));
        }

        public InputDeclarer noneGrouping(String componentId) {
            return noneGrouping(componentId, Utils.DEFAULT_STREAM_ID);
        }

        public InputDeclarer noneGrouping(String componentId, String streamId) {
            return grouping(componentId, streamId, Grouping.none(new NullStruct()));
        }

        public InputDeclarer allGrouping(String componentId) {
            return allGrouping(componentId, Utils.DEFAULT_STREAM_ID);
        }

        public InputDeclarer allGrouping(String componentId, String streamId) {
            return grouping(componentId, streamId, Grouping.all(new NullStruct()));
        }

        public InputDeclarer directGrouping(String componentId) {
            return directGrouping(componentId, Utils.DEFAULT_STREAM_ID);
        }

        public InputDeclarer directGrouping(String componentId, String streamId) {
            return grouping(componentId, streamId, Grouping.direct(new NullStruct()));
        }

        private InputDeclarer grouping(String componentId, String streamId, Grouping grouping) {
            _inputs.get(_boltId).put(new GlobalStreamId(componentId, streamId), grouping);
            return this;
        }

        @Override
        public InputDeclarer customGrouping(String componentId, CustomStreamGrouping grouping) {
            return customGrouping(componentId, Utils.DEFAULT_STREAM_ID, grouping);
        }

        @Override
        public InputDeclarer customGrouping(String componentId, String streamId, CustomStreamGrouping grouping) {
            return grouping(componentId, streamId, Grouping.custom_serialized(Utils.serialize(grouping)));
        }
        
    }
}
