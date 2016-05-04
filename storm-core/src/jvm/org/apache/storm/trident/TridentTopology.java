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

import org.apache.storm.Config;
import org.apache.storm.ILocalDRPC;
import org.apache.storm.drpc.DRPCSpout;
import org.apache.storm.generated.GlobalStreamId;
import org.apache.storm.generated.Grouping;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.grouping.CustomStreamGrouping;
import org.apache.storm.topology.BoltDeclarer;
import org.apache.storm.topology.IRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.utils.Utils;

import java.io.Serializable;
import java.util.*;

import org.jgrapht.DirectedGraph;
import org.jgrapht.UndirectedGraph;
import org.jgrapht.alg.ConnectivityInspector;
import org.jgrapht.graph.DefaultDirectedGraph;
import org.jgrapht.graph.Pseudograph;
import org.apache.storm.trident.drpc.ReturnResultsReducer;
import org.apache.storm.trident.fluent.GroupedStream;
import org.apache.storm.trident.fluent.IAggregatableStream;
import org.apache.storm.trident.fluent.UniqueIdGen;
import org.apache.storm.trident.graph.GraphGrouper;
import org.apache.storm.trident.graph.Group;
import org.apache.storm.trident.operation.GroupedMultiReducer;
import org.apache.storm.trident.operation.ITridentResource;
import org.apache.storm.trident.operation.MultiReducer;
import org.apache.storm.trident.operation.impl.FilterExecutor;
import org.apache.storm.trident.operation.impl.GroupedMultiReducerExecutor;
import org.apache.storm.trident.operation.impl.IdentityMultiReducer;
import org.apache.storm.trident.operation.impl.JoinerMultiReducer;
import org.apache.storm.trident.operation.impl.TrueFilter;
import org.apache.storm.trident.partition.IdentityGrouping;
import org.apache.storm.trident.planner.Node;
import org.apache.storm.trident.planner.NodeStateInfo;
import org.apache.storm.trident.planner.PartitionNode;
import org.apache.storm.trident.planner.ProcessorNode;
import org.apache.storm.trident.planner.SpoutNode;
import org.apache.storm.trident.planner.SubtopologyBolt;
import org.apache.storm.trident.planner.processor.EachProcessor;
import org.apache.storm.trident.planner.processor.MultiReducerProcessor;
import org.apache.storm.trident.spout.BatchSpoutExecutor;
import org.apache.storm.trident.spout.IBatchSpout;
import org.apache.storm.trident.spout.IOpaquePartitionedTridentSpout;
import org.apache.storm.trident.spout.IPartitionedTridentSpout;
import org.apache.storm.trident.spout.ITridentDataSource;
import org.apache.storm.trident.spout.ITridentSpout;
import org.apache.storm.trident.spout.OpaquePartitionedTridentSpoutExecutor;
import org.apache.storm.trident.spout.PartitionedTridentSpoutExecutor;
import org.apache.storm.trident.spout.RichSpoutBatchExecutor;
import org.apache.storm.trident.state.StateFactory;
import org.apache.storm.trident.state.StateSpec;
import org.apache.storm.trident.topology.TridentTopologyBuilder;
import org.apache.storm.trident.util.ErrorEdgeFactory;
import org.apache.storm.trident.util.IndexedEdge;
import org.apache.storm.trident.util.TridentUtils;


// graph with 3 kinds of nodes:
// operation, partition, or spout
// all operations have finishBatch and can optionally be committers
public class TridentTopology {
    
    //TODO: add a method for drpc stream, needs to know how to automatically do return results, etc
    // is it too expensive to do a batch per drpc request?
    
    final DefaultDirectedGraph<Node, IndexedEdge> _graph;
    final Map<String, List<Node>> _colocate;
    final UniqueIdGen _gen;

    public TridentTopology() {
        this(new DefaultDirectedGraph<Node, IndexedEdge>(new ErrorEdgeFactory()),
                new LinkedHashMap<String, List<Node>>(),
                new UniqueIdGen());
    }
    
    private TridentTopology(DefaultDirectedGraph<Node, IndexedEdge> graph, Map<String, List<Node>> colocate, UniqueIdGen gen) {
        _graph = graph;
        _colocate = colocate;
        _gen = gen;
    }
    
    
    // automatically turn it into a batch spout, should take parameters as to how much to batch
//    public Stream newStream(IRichSpout spout) {
//        Node n = new SpoutNode(getUniqueStreamId(), TridentUtils.getSingleOutputStreamFields(spout), null, spout, SpoutNode.SpoutType.BATCH);
//        return addNode(n);
//    }
    
     public Stream newStream(String txId, IRichSpout spout) {
        return newStream(txId, new RichSpoutBatchExecutor(spout));
    }
    
    public Stream newStream(String txId, IBatchSpout spout) {
        Node n = new SpoutNode(getUniqueStreamId(), spout.getOutputFields(), txId, spout, SpoutNode.SpoutType.BATCH);
        return addNode(n);
    }
    
    public Stream newStream(String txId, ITridentSpout spout) {
        Node n = new SpoutNode(getUniqueStreamId(), spout.getOutputFields(), txId, spout, SpoutNode.SpoutType.BATCH);
        return addNode(n);
    }
    
    public Stream newStream(String txId, IPartitionedTridentSpout spout) {
        return newStream(txId, new PartitionedTridentSpoutExecutor(spout));
    }
    
    public Stream newStream(String txId, IOpaquePartitionedTridentSpout spout) {
        return newStream(txId, new OpaquePartitionedTridentSpoutExecutor(spout));
    }

    public Stream newStream(String txId, ITridentDataSource dataSource) {
        if (dataSource instanceof IBatchSpout) {
            return newStream(txId, (IBatchSpout) dataSource);
        } else if (dataSource instanceof ITridentSpout) {
            return newStream(txId, (ITridentSpout) dataSource);
        } else if (dataSource instanceof IPartitionedTridentSpout) {
            return newStream(txId, (IPartitionedTridentSpout) dataSource);
        } else if (dataSource instanceof IOpaquePartitionedTridentSpout) {
            return newStream(txId, (IOpaquePartitionedTridentSpout) dataSource);
        } else {
            throw new UnsupportedOperationException("Unsupported stream");
        }
    }

    public Stream newDRPCStream(String function) {
        return newDRPCStream(new DRPCSpout(function));
    }

    public Stream newDRPCStream(String function, ILocalDRPC server) {
        DRPCSpout spout;
        if(server==null) {
            spout = new DRPCSpout(function);
        } else {
            spout = new DRPCSpout(function, server);
        }
        return newDRPCStream(spout);
    }
    
    private Stream newDRPCStream(DRPCSpout spout) {
        // TODO: consider adding a shuffle grouping after the spout to avoid so much routing of the args/return-info all over the place
        // (at least until its possible to just pack bolt logic into the spout itself)

        Node n = new SpoutNode(getUniqueStreamId(), TridentUtils.getSingleOutputStreamFields(spout), null, spout, SpoutNode.SpoutType.DRPC);
        Stream nextStream = addNode(n);
        // later on, this will be joined back with return-info and all the results
        return nextStream.project(new Fields("args"));
    }
    
    public TridentState newStaticState(StateFactory factory) {
        return newStaticState(new StateSpec(factory));
    }
    
    public TridentState newStaticState(StateSpec spec) {
        String stateId = getUniqueStateId();
        Node n = new Node(getUniqueStreamId(), null, new Fields());
        n.stateInfo = new NodeStateInfo(stateId, spec);
        registerNode(n);
        return new TridentState(this, n);
    }
    
    public Stream multiReduce(Stream s1, Stream s2, MultiReducer function, Fields outputFields) {
        return multiReduce(Arrays.asList(s1, s2), function, outputFields);        
    }

    public Stream multiReduce(Fields inputFields1, Stream s1, Fields inputFields2, Stream s2, MultiReducer function, Fields outputFields) {
        return multiReduce(Arrays.asList(inputFields1, inputFields2), Arrays.asList(s1, s2), function, outputFields);        
    }    
    
    public Stream multiReduce(GroupedStream s1, GroupedStream s2, GroupedMultiReducer function, Fields outputFields) {
        return multiReduce(Arrays.asList(s1, s2), function, outputFields);        
    }
    
    public Stream multiReduce(Fields inputFields1, GroupedStream s1, Fields inputFields2, GroupedStream s2, GroupedMultiReducer function, Fields outputFields) {
        return multiReduce(Arrays.asList(inputFields1, inputFields2), Arrays.asList(s1, s2), function, outputFields);        
    } 
    
    public Stream multiReduce(List<Stream> streams, MultiReducer function, Fields outputFields) {
        return multiReduce(getAllOutputFields(streams), streams, function, outputFields);
    }
        
    public Stream multiReduce(List<GroupedStream> streams, GroupedMultiReducer function, Fields outputFields) {
        return multiReduce(getAllOutputFields(streams), streams, function, outputFields);        
    }    
    
    public Stream multiReduce(List<Fields> inputFields, List<Stream> streams, MultiReducer function, Fields outputFields) {
        List<String> names = new ArrayList<>();
        for(Stream s: streams) {
            if(s._name!=null) {
                names.add(s._name);
            }
        }
        Node n = new ProcessorNode(getUniqueStreamId(), Utils.join(names, "-"), outputFields, outputFields, new MultiReducerProcessor(inputFields, function));
        return addSourcedNode(streams, n);
    }
    
    public Stream multiReduce(List<Fields> inputFields, List<GroupedStream> groupedStreams, GroupedMultiReducer function, Fields outputFields) {
        List<Fields> fullInputFields = new ArrayList<>();
        List<Stream> streams = new ArrayList<>();
        List<Fields> fullGroupFields = new ArrayList<>();
        for(int i=0; i<groupedStreams.size(); i++) {
            GroupedStream gs = groupedStreams.get(i);
            Fields groupFields = gs.getGroupFields();
            fullGroupFields.add(groupFields);
            streams.add(gs.toStream().partitionBy(groupFields));
            fullInputFields.add(TridentUtils.fieldsUnion(groupFields, inputFields.get(i)));
            
        }
        return multiReduce(fullInputFields, streams, new GroupedMultiReducerExecutor(function, fullGroupFields, inputFields), outputFields);
    }
    
    public Stream merge(Fields outputFields, Stream... streams) {
        return merge(outputFields, Arrays.asList(streams));
    }
    
    public Stream merge(Fields outputFields, List<Stream> streams) {
        return multiReduce(streams, new IdentityMultiReducer(), outputFields);
    }
    
    public Stream merge(Stream... streams) {
        return merge(Arrays.asList(streams));
    }
    
    public Stream merge(List<Stream> streams) {
        return merge(streams.get(0).getOutputFields(), streams);
    } 
    
    public Stream join(Stream s1, Fields joinFields1, Stream s2, Fields joinFields2, Fields outFields) {
        return join(Arrays.asList(s1, s2), Arrays.asList(joinFields1, joinFields2), outFields);        
    }
    
    public Stream join(List<Stream> streams, List<Fields> joinFields, Fields outFields) {
        return join(streams, joinFields, outFields, JoinType.INNER);        
    }

    public Stream join(Stream s1, Fields joinFields1, Stream s2, Fields joinFields2, Fields outFields, JoinType type) {
        return join(Arrays.asList(s1, s2), Arrays.asList(joinFields1, joinFields2), outFields, type);        
    }
    
    public Stream join(List<Stream> streams, List<Fields> joinFields, Fields outFields, JoinType type) {
        return join(streams, joinFields, outFields, repeat(streams.size(), type));        
    }

    public Stream join(Stream s1, Fields joinFields1, Stream s2, Fields joinFields2, Fields outFields, List<JoinType> mixed) {
        return join(Arrays.asList(s1, s2), Arrays.asList(joinFields1, joinFields2), outFields, mixed);        
        
    }
    
    public Stream join(List<Stream> streams, List<Fields> joinFields, Fields outFields, List<JoinType> mixed) {
        return multiReduce(strippedInputFields(streams, joinFields),
              groupedStreams(streams, joinFields),
              new JoinerMultiReducer(mixed, joinFields.get(0).size(), strippedInputFields(streams, joinFields)),
              outFields);
    }    
        
    public StormTopology build() {
        DefaultDirectedGraph<Node, IndexedEdge> graph = (DefaultDirectedGraph) _graph.clone();
        
        
        completeDRPC(graph, _colocate, _gen);
        
        List<SpoutNode> spoutNodes = new ArrayList<>();
        
        // can be regular nodes (static state) or processor nodes
        Set<Node> boltNodes = new LinkedHashSet<>();
        for(Node n: graph.vertexSet()) {
            if(n instanceof SpoutNode) {
                spoutNodes.add((SpoutNode) n);
            } else if(!(n instanceof PartitionNode)) {
                boltNodes.add(n);
            }
        }
        
        
        Set<Group> initialGroups = new LinkedHashSet<>();
        for(List<Node> colocate: _colocate.values()) {
            Group g = new Group(graph, colocate);
            boltNodes.removeAll(colocate);
            initialGroups.add(g);
        }
        for(Node n: boltNodes) {
            initialGroups.add(new Group(graph, n));
        }
        
        
        GraphGrouper grouper = new GraphGrouper(graph, initialGroups);
        grouper.mergeFully();
        Collection<Group> mergedGroups = grouper.getAllGroups();
        
        
        
        // add identity partitions between groups
        for(IndexedEdge<Node> e: new HashSet<>(graph.edgeSet())) {
            if(!(e.source instanceof PartitionNode) && !(e.target instanceof PartitionNode)) {                
                Group g1 = grouper.nodeGroup(e.source);
                Group g2 = grouper.nodeGroup(e.target);
                // g1 being null means the source is a spout node
                if(g1==null && !(e.source instanceof SpoutNode))
                    throw new RuntimeException("Planner exception: Null source group must indicate a spout node at this phase of planning");
                if(g1==null || !g1.equals(g2)) {
                    graph.removeEdge(e);
                    PartitionNode pNode = makeIdentityPartition(e.source);
                    graph.addVertex(pNode);
                    graph.addEdge(e.source, pNode, new IndexedEdge(e.source, pNode, 0));
                    graph.addEdge(pNode, e.target, new IndexedEdge(pNode, e.target, e.index));                    
                }
            }
        }
        // if one group subscribes to the same stream with same partitioning multiple times,
        // merge those together (otherwise can end up with many output streams created for that partitioning
        // if need to split into multiple output streams because of same input having different
        // partitioning to the group)
        
        // this is because can't currently merge splitting logic into a spout
        // not the most kosher algorithm here, since the grouper indexes are being trounced via the adding of nodes to random groups, but it 
        // works out
        List<Node> forNewGroups = new ArrayList<>();
        for(Group g: mergedGroups) {
            for(PartitionNode n: extraPartitionInputs(g)) {
                Node idNode = makeIdentityNode(n.allOutputFields);
                Node newPartitionNode = new PartitionNode(idNode.streamId, n.name, idNode.allOutputFields, n.thriftGrouping);
                Node parentNode = TridentUtils.getParent(graph, n);
                Set<IndexedEdge> outgoing = graph.outgoingEdgesOf(n);
                graph.removeVertex(n);
                
                graph.addVertex(idNode);
                graph.addVertex(newPartitionNode);
                addEdge(graph, parentNode, idNode, 0);
                addEdge(graph, idNode, newPartitionNode, 0);
                for(IndexedEdge e: outgoing) {
                    addEdge(graph, newPartitionNode, e.target, e.index);
                }
                Group parentGroup = grouper.nodeGroup(parentNode);
                if(parentGroup==null) {
                    forNewGroups.add(idNode);
                } else {
                    parentGroup.nodes.add(idNode);
                }
            }
        }
        // TODO: in the future, want a way to include this logic in the spout itself,
        // or make it unnecessary by having storm include metadata about which grouping a tuple
        // came from
        
        for(Node n: forNewGroups) {
            grouper.addGroup(new Group(graph, n));
        }
        
        // add in spouts as groups so we can get parallelisms
        for(Node n: spoutNodes) {
            grouper.addGroup(new Group(graph, n));
        }
        
        grouper.reindex();
        mergedGroups = grouper.getAllGroups();
                
        
        Map<Node, String> batchGroupMap = new HashMap<>();
        List<Set<Node>> connectedComponents = new ConnectivityInspector<>(graph).connectedSets();
        for(int i=0; i<connectedComponents.size(); i++) {
            String groupId = "bg" + i;
            for(Node n: connectedComponents.get(i)) {
                batchGroupMap.put(n, groupId);
            }
        }
        
//        System.out.println("GRAPH:");
//        System.out.println(graph);
        
        Map<Group, Integer> parallelisms = getGroupParallelisms(graph, grouper, mergedGroups);
        
        TridentTopologyBuilder builder = new TridentTopologyBuilder();
        
        Map<Node, String> spoutIds = genSpoutIds(spoutNodes);
        Map<Group, String> boltIds = genBoltIds(mergedGroups);
        
        Map defaults = Utils.readDefaultConfig();

        for(SpoutNode sn: spoutNodes) {
            Integer parallelism = parallelisms.get(grouper.nodeGroup(sn));

            Map<String, Number> spoutRes = null;
            spoutRes = mergeDefaultResources(sn.getResources(), defaults);
            Number onHeap = spoutRes.get(Config.TOPOLOGY_COMPONENT_RESOURCES_ONHEAP_MEMORY_MB);
            Number offHeap = spoutRes.get(Config.TOPOLOGY_COMPONENT_RESOURCES_OFFHEAP_MEMORY_MB);
            Number cpuLoad = spoutRes.get(Config.TOPOLOGY_COMPONENT_CPU_PCORE_PERCENT);

            if(sn.type == SpoutNode.SpoutType.DRPC) {

                builder.setBatchPerTupleSpout(spoutIds.get(sn), sn.streamId,
                                              (IRichSpout) sn.spout, parallelism, batchGroupMap.get(sn))
                    .setMemoryLoad(onHeap, offHeap)
                    .setCPULoad(cpuLoad);
            } else {
                ITridentSpout s;
                if(sn.spout instanceof IBatchSpout) {
                    s = new BatchSpoutExecutor((IBatchSpout)sn.spout);
                } else if(sn.spout instanceof ITridentSpout) {
                    s = (ITridentSpout) sn.spout;
                } else {
                    throw new RuntimeException("Regular rich spouts not supported yet... try wrapping in a RichSpoutBatchExecutor");
                    // TODO: handle regular rich spout without batches (need lots of updates to support this throughout)
                }
                builder.setSpout(spoutIds.get(sn), sn.streamId, sn.txId, s, parallelism, batchGroupMap.get(sn))
                    .setMemoryLoad(onHeap, offHeap)
                    .setCPULoad(cpuLoad);
            }
        }

        for(Group g: mergedGroups) {
            if(!isSpoutGroup(g)) {
                Integer p = parallelisms.get(g);
                Map<String, String> streamToGroup = getOutputStreamBatchGroups(g, batchGroupMap);
                Map<String, Number> groupRes = mergeDefaultResources(g.getResources(), defaults);

                Number onHeap = groupRes.get(Config.TOPOLOGY_COMPONENT_RESOURCES_ONHEAP_MEMORY_MB);
                Number offHeap = groupRes.get(Config.TOPOLOGY_COMPONENT_RESOURCES_OFFHEAP_MEMORY_MB);
                Number cpuLoad = groupRes.get(Config.TOPOLOGY_COMPONENT_CPU_PCORE_PERCENT);

                BoltDeclarer d = builder.setBolt(boltIds.get(g), new SubtopologyBolt(graph, g.nodes, batchGroupMap), p,
                                                 committerBatches(g, batchGroupMap), streamToGroup)
                    .setMemoryLoad(onHeap, offHeap)
                    .setCPULoad(cpuLoad);
                Collection<PartitionNode> inputs = uniquedSubscriptions(externalGroupInputs(g));
                for(PartitionNode n: inputs) {
                    Node parent = TridentUtils.getParent(graph, n);
                    String componentId = parent instanceof SpoutNode ?
                            spoutIds.get(parent) : boltIds.get(grouper.nodeGroup(parent));
                    d.grouping(new GlobalStreamId(componentId, n.streamId), n.thriftGrouping);
                }
            }
        }
        
        return builder.buildTopology();
    }

    private static Map<String, Number> mergeDefaultResources(Map<String, Number> res, Map defaultConfig) {
        Map<String, Number> ret = new HashMap<String, Number>();

        Number onHeapDefault = (Number)defaultConfig.get(Config.TOPOLOGY_COMPONENT_RESOURCES_ONHEAP_MEMORY_MB);
        Number offHeapDefault = (Number)defaultConfig.get(Config.TOPOLOGY_COMPONENT_RESOURCES_OFFHEAP_MEMORY_MB);
        Number cpuLoadDefault = (Number)defaultConfig.get(Config.TOPOLOGY_COMPONENT_CPU_PCORE_PERCENT);

        if(res == null) {
            ret.put(Config.TOPOLOGY_COMPONENT_RESOURCES_ONHEAP_MEMORY_MB, onHeapDefault);
            ret.put(Config.TOPOLOGY_COMPONENT_RESOURCES_OFFHEAP_MEMORY_MB, offHeapDefault);
            ret.put(Config.TOPOLOGY_COMPONENT_CPU_PCORE_PERCENT, cpuLoadDefault);
            return ret;
        }

        Number onHeap = res.get(Config.TOPOLOGY_COMPONENT_RESOURCES_ONHEAP_MEMORY_MB);
        Number offHeap = res.get(Config.TOPOLOGY_COMPONENT_RESOURCES_OFFHEAP_MEMORY_MB);
        Number cpuLoad = res.get(Config.TOPOLOGY_COMPONENT_CPU_PCORE_PERCENT);

        /* We take the max of the default and whatever the user put in here.
           Each node's resources can be the sum of several operations, so the simplest
           thing to do is get the max.

           The situation we want to avoid is that the user sets low resources on one
           node, and when that node is combined with a bunch of others, the sum is still
           that low resource count. If any component isn't set, we want to use the default.

           Right now, this code does not check that. It just takes the max of the summed
           up resource counts for simplicity's sake. We could perform some more complicated
           logic to be more accurate, but the benefits are very small, and only apply to some
           very odd corner cases. */
        if(onHeap == null) {
            onHeap = onHeapDefault;
        }
        else {
            onHeap = Math.max(onHeap.doubleValue(), onHeapDefault.doubleValue());
        }

        if(offHeap == null) {
            offHeap = offHeapDefault;
        }
        else {
            offHeap = Math.max(offHeap.doubleValue(), offHeapDefault.doubleValue());
        }

        if(cpuLoad == null) {
            cpuLoad = cpuLoadDefault;
        }
        else {
            cpuLoad = Math.max(cpuLoad.doubleValue(), cpuLoadDefault.doubleValue());
        }

        ret.put(Config.TOPOLOGY_COMPONENT_RESOURCES_ONHEAP_MEMORY_MB, onHeap);
        ret.put(Config.TOPOLOGY_COMPONENT_RESOURCES_OFFHEAP_MEMORY_MB, offHeap);
        ret.put(Config.TOPOLOGY_COMPONENT_CPU_PCORE_PERCENT, cpuLoad);

        return ret;
    }
    
    private static void completeDRPC(DefaultDirectedGraph<Node, IndexedEdge> graph, Map<String, List<Node>> colocate, UniqueIdGen gen) {
        List<Set<Node>> connectedComponents = new ConnectivityInspector<>(graph).connectedSets();
        
        for(Set<Node> g: connectedComponents) {
            checkValidJoins(g);
        }
        
        TridentTopology helper = new TridentTopology(graph, colocate, gen);
        for(Set<Node> g: connectedComponents) {
            SpoutNode drpcNode = getDRPCSpoutNode(g);
            if(drpcNode!=null) {
                Stream lastStream = new Stream(helper, null, getLastAddedNode(g));
                Stream s = new Stream(helper, null, drpcNode);
                helper.multiReduce(
                        s.project(new Fields("return-info"))
                         .batchGlobal(),
                        lastStream.batchGlobal(),
                        new ReturnResultsReducer(),
                        new Fields());
            }
        }                
    }
    
    private static Node getLastAddedNode(Collection<Node> g) {
        Node ret = null;
        for(Node n: g) {
            if(ret==null || n.creationIndex > ret.creationIndex) {
                ret = n;
            }
        }
        return ret;
    }

    //returns null if it's not a drpc group
    private static SpoutNode getDRPCSpoutNode(Collection<Node> g) {
        for(Node n: g) {
            if(n instanceof SpoutNode) {
                SpoutNode.SpoutType type = ((SpoutNode) n).type;
                if(type==SpoutNode.SpoutType.DRPC) {
                    return (SpoutNode) n;
                }
            }
        }
        return null;
    }
    
    private static void checkValidJoins(Collection<Node> g) {
        boolean hasDRPCSpout = false;
        boolean hasBatchSpout = false;
        for(Node n: g) {
            if(n instanceof SpoutNode) {
                SpoutNode.SpoutType type = ((SpoutNode) n).type;
                if(type==SpoutNode.SpoutType.BATCH) {
                    hasBatchSpout = true;
                } else if(type==SpoutNode.SpoutType.DRPC) {
                    hasDRPCSpout = true;
                }
            }
        }
        if(hasBatchSpout && hasDRPCSpout) {
            throw new RuntimeException("Cannot join DRPC stream with streams originating from other spouts");
        }
    }
    
    private static boolean isSpoutGroup(Group g) {
        return g.nodes.size() == 1 && g.nodes.iterator().next() instanceof SpoutNode;
    }
    
    private static Collection<PartitionNode> uniquedSubscriptions(Set<PartitionNode> subscriptions) {
        Map<String, PartitionNode> ret = new HashMap<>();
        for(PartitionNode n: subscriptions) {
            PartitionNode curr = ret.get(n.streamId);
            if(curr!=null && !curr.thriftGrouping.equals(n.thriftGrouping)) {
                throw new RuntimeException("Multiple subscriptions to the same stream with different groupings. Should be impossible since that is explicitly guarded against.");
            }
            ret.put(n.streamId, n);
        }
        return ret.values();
    }

    private static Map<Node, String> genSpoutIds(Collection<SpoutNode> spoutNodes) {
        Map<Node, String> ret = new HashMap<>();
        int ctr = 0;
        for(SpoutNode n: spoutNodes) {
            if (n.type == SpoutNode.SpoutType.BATCH) { // if Batch spout then id contains txId
                ret.put(n, "spout-" + n.txId);
            } else if (n.type == SpoutNode.SpoutType.DRPC){ //if DRPC spout then id contains function
                ret.put(n, "spout-" + ((DRPCSpout) n.spout).get_function() + ctr);
                ctr++;
            } else {
                ret.put(n, "spout" + ctr);
                ctr++;
            }
        }
        return ret;
    }

    private static Map<Group, String> genBoltIds(Collection<Group> groups) {
        Map<Group, String> ret = new HashMap<>();
        int ctr = 0;
        for(Group g: groups) {
            if(!isSpoutGroup(g)) {
                List<String> name = new ArrayList<>();
                name.add("b");
                name.add("" + ctr);
                String groupName = getGroupName(g);
                if(groupName!=null && !groupName.isEmpty()) {
                    name.add(getGroupName(g));                
                }
                ret.put(g, Utils.join(name, "-"));
                ctr++;
            }
        }
        return ret;
    }
    
    private static String getGroupName(Group g) {
        TreeMap<Integer, String> sortedNames = new TreeMap<>();
        for(Node n: g.nodes) {
            if(n.name!=null) {
                sortedNames.put(n.creationIndex, n.name);
            }
        }
        List<String> names = new ArrayList<>();
        String prevName = null;
        for(String n: sortedNames.values()) {
            if(prevName==null || !n.equals(prevName)) {
                prevName = n;
                names.add(n);
            }
        }
        return Utils.join(names, "-");
    }
    
    private static Map<String, String> getOutputStreamBatchGroups(Group g, Map<Node, String> batchGroupMap) {
        Map<String, String> ret = new HashMap<>();
        Set<PartitionNode> externalGroupOutputs = externalGroupOutputs(g);
        for(PartitionNode n: externalGroupOutputs) {
            ret.put(n.streamId, batchGroupMap.get(n));
        }        
        return ret;
    }
    
    private static Set<String> committerBatches(Group g, Map<Node, String> batchGroupMap) {
        Set<String> ret = new HashSet<>();
        for(Node n: g.nodes) {
           if(n instanceof ProcessorNode) {
               if(((ProcessorNode) n).committer) {
                   ret.add(batchGroupMap.get(n));
               }
           } 
        }
        return ret;
    }
    
    private static Map<Group, Integer> getGroupParallelisms(DirectedGraph<Node, IndexedEdge> graph, GraphGrouper grouper, Collection<Group> groups) {
        UndirectedGraph<Group, Object> equivs = new Pseudograph<>(Object.class);
        for(Group g: groups) {
            equivs.addVertex(g);
        }
        for(Group g: groups) {
            for(PartitionNode n: externalGroupInputs(g)) {
                if(isIdentityPartition(n)) {
                    Node parent = TridentUtils.getParent(graph, n);
                    Group parentGroup = grouper.nodeGroup(parent);
                    if(parentGroup!=null && !parentGroup.equals(g)) {
                        equivs.addEdge(parentGroup, g);
                    }
                }
            }            
        }
        
        Map<Group, Integer> ret = new HashMap<>();
        List<Set<Group>> equivGroups = new ConnectivityInspector<>(equivs).connectedSets();
        for(Set<Group> equivGroup: equivGroups) {
            Integer fixedP = getFixedParallelism(equivGroup);
            Integer maxP = getMaxParallelism(equivGroup);
            if(fixedP!=null && maxP!=null && maxP < fixedP) {
                throw new RuntimeException("Parallelism is fixed to " + fixedP + " but max parallelism is less than that: " + maxP);
            }
            
            
            Integer p = 1;
            for(Group g: equivGroup) {
                for(Node n: g.nodes) {
                    if(n.parallelismHint!=null) {
                        p = Math.max(p, n.parallelismHint);
                    }
                }
            }
            if(maxP!=null) p = Math.min(maxP, p);
            
            if(fixedP!=null) p = fixedP;
            for(Group g: equivGroup) {
                ret.put(g, p);
            }
        }
        return ret;
    }
    
    private static Integer getMaxParallelism(Set<Group> groups) {
        Integer ret = null;
        for(Group g: groups) {
            if(isSpoutGroup(g)) {
                SpoutNode n = (SpoutNode) g.nodes.iterator().next();
                Map conf = getSpoutComponentConfig(n.spout);
                if(conf==null) conf = new HashMap();
                Number maxP = (Number) conf.get(Config.TOPOLOGY_MAX_TASK_PARALLELISM);
                if(maxP!=null) {
                    if(ret==null) ret = maxP.intValue();
                    else ret = Math.min(ret, maxP.intValue());
                }
            }
        }
        return ret;
    }
    
    private static Map getSpoutComponentConfig(Object spout) {
        if(spout instanceof IRichSpout) {
            return ((IRichSpout) spout).getComponentConfiguration();
        } else if (spout instanceof IBatchSpout) {
            return ((IBatchSpout) spout).getComponentConfiguration();
        } else {
            return ((ITridentSpout) spout).getComponentConfiguration();
        }
    }
    
    private static Integer getFixedParallelism(Set<Group> groups) {
        Integer ret = null;
        for(Group g: groups) {
            for(Node n: g.nodes) {
                if(n.stateInfo != null && n.stateInfo.spec.requiredNumPartitions!=null) {
                    int reqPartitions = n.stateInfo.spec.requiredNumPartitions;
                    if(ret!=null && ret!=reqPartitions) {
                        throw new RuntimeException("Cannot have one group have fixed parallelism of two different values");
                    }
                    ret = reqPartitions;
                }
            }
        }
        return ret;
    }
    
    private static boolean isIdentityPartition(PartitionNode n) {
        Grouping g = n.thriftGrouping;
        if(g.is_set_custom_serialized()) {
            CustomStreamGrouping csg = (CustomStreamGrouping) Utils.javaDeserialize(g.get_custom_serialized(), Serializable.class);
            return csg instanceof IdentityGrouping;
        }
        return false;
    }
    
    private static void addEdge(DirectedGraph g, Object source, Object target, int index) {
        g.addEdge(source, target, new IndexedEdge(source, target, index));
    }
    
    private Node makeIdentityNode(Fields allOutputFields) {
        return new ProcessorNode(getUniqueStreamId(), null, allOutputFields, new Fields(),
                new EachProcessor(new Fields(), new FilterExecutor(new TrueFilter())));
    }
    
    private static List<PartitionNode> extraPartitionInputs(Group g) {
        List<PartitionNode> ret = new ArrayList<>();
        Set<PartitionNode> inputs = externalGroupInputs(g);
        Map<String, List<PartitionNode>> grouped = new HashMap<>();
        for(PartitionNode n: inputs) {
            if(!grouped.containsKey(n.streamId)) {
                grouped.put(n.streamId, new ArrayList());
            }
            grouped.get(n.streamId).add(n);
        }
        for(List<PartitionNode> group: grouped.values()) {
            PartitionNode anchor = group.get(0);
            for(int i=1; i<group.size(); i++) {
                PartitionNode n = group.get(i);
                if(!n.thriftGrouping.equals(anchor.thriftGrouping)) {
                    ret.add(n);
                }
            }
        }
        return ret;
    }
    
    private static Set<PartitionNode> externalGroupInputs(Group g) {
        Set<PartitionNode> ret = new HashSet<>();
        for(Node n: g.incomingNodes()) {
            if(n instanceof PartitionNode) {
                ret.add((PartitionNode) n);
            }
        }
        return ret;
    }
    
    private static Set<PartitionNode> externalGroupOutputs(Group g) {
        Set<PartitionNode> ret = new HashSet<>();
        for(Node n: g.outgoingNodes()) {
            if(n instanceof PartitionNode) {
                ret.add((PartitionNode) n);
            }
        }
        return ret;
    }    
    
    private static PartitionNode makeIdentityPartition(Node basis) {
        return new PartitionNode(basis.streamId, basis.name, basis.allOutputFields,
            Grouping.custom_serialized(Utils.javaSerialize(new IdentityGrouping())));
    }
    
    
    protected String getUniqueStreamId() {
        return _gen.getUniqueStreamId();
    }

    protected String getUniqueStateId() {
        return _gen.getUniqueStateId();
    }

    protected String getUniqueWindowId() {
        return _gen.getUniqueWindowId();
    }
    
    protected void registerNode(Node n) {
        _graph.addVertex(n);
        if(n.stateInfo!=null) {
            String id = n.stateInfo.id;
            if(!_colocate.containsKey(id)) {
                _colocate.put(id, new ArrayList());
            }
            _colocate.get(id).add(n);
        }
    }
    
    protected Stream addNode(Node n) {
        registerNode(n);
        return new Stream(this, n.name, n);
    }

    protected void registerSourcedNode(List<Stream> sources, Node newNode) {
        registerNode(newNode);
        int streamIndex = 0;
        for(Stream s: sources) {
            _graph.addEdge(s._node, newNode, new IndexedEdge(s._node, newNode, streamIndex));
            streamIndex++;
        }        
    }
    
    protected Stream addSourcedNode(List<Stream> sources, Node newNode) {
        registerSourcedNode(sources, newNode);
        return new Stream(this, newNode.name, newNode);
    }
    
    protected TridentState addSourcedStateNode(List<Stream> sources, Node newNode) {
        registerSourcedNode(sources, newNode);
        return new TridentState(this, newNode);
    }    
    
    protected Stream addSourcedNode(Stream source, Node newNode) {
        return addSourcedNode(Arrays.asList(source), newNode);
    }

    protected TridentState addSourcedStateNode(Stream source, Node newNode) {
        return addSourcedStateNode(Arrays.asList(source), newNode);
    }       
    
    private static List<Fields> getAllOutputFields(List streams) {
        List<Fields> ret = new ArrayList<>();
        for(Object o: streams) {
            ret.add(((IAggregatableStream) o).getOutputFields());
        }
        return ret;
    }
    
    
    private static List<GroupedStream> groupedStreams(List<Stream> streams, List<Fields> joinFields) {
        List<GroupedStream> ret = new ArrayList<>();
        for(int i=0; i<streams.size(); i++) {
            ret.add(streams.get(i).groupBy(joinFields.get(i)));
        }
        return ret;
    }
    
    private static List<Fields> strippedInputFields(List<Stream> streams, List<Fields> joinFields) {
        List<Fields> ret = new ArrayList<>();
        for(int i=0; i<streams.size(); i++) {
            ret.add(TridentUtils.fieldsSubtract(streams.get(i).getOutputFields(), joinFields.get(i)));
        }
        return ret;
    }
    
    private static List<JoinType> repeat(int n, JoinType type) {
        List<JoinType> ret = new ArrayList<>();
        for(int i=0; i<n; i++) {
            ret.add(type);
        }
        return ret;
    }
}
