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
package storm.trident.topology;

import backtype.storm.generated.GlobalStreamId;
import backtype.storm.generated.Grouping;
import backtype.storm.generated.StormTopology;
import backtype.storm.grouping.CustomStreamGrouping;
import backtype.storm.topology.BaseConfigurationDeclarer;
import backtype.storm.topology.BoltDeclarer;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.InputDeclarer;
import backtype.storm.topology.SpoutDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.commons.lang.builder.ToStringBuilder;
import org.apache.commons.lang.builder.ToStringStyle;
import storm.trident.spout.BatchSpoutExecutor;
import storm.trident.spout.IBatchSpout;
import storm.trident.spout.ICommitterTridentSpout;
import storm.trident.spout.ITridentSpout;
import storm.trident.spout.RichSpoutBatchTriggerer;
import storm.trident.spout.TridentSpoutCoordinator;
import storm.trident.spout.TridentSpoutExecutor;
import storm.trident.topology.TridentBoltExecutor.CoordSpec;
import storm.trident.topology.TridentBoltExecutor.CoordType;

// based on transactional topologies
public class TridentTopologyBuilder {
    Map<GlobalStreamId, String> _batchIds = new HashMap();
    Map<String, TransactionalSpoutComponent> _spouts = new HashMap();
    Map<String, SpoutComponent> _batchPerTupleSpouts = new HashMap();
    Map<String, Component> _bolts = new HashMap();
        
    
    public SpoutDeclarer setBatchPerTupleSpout(String id, String streamName, IRichSpout spout, Integer parallelism, String batchGroup) {
        Map<String, String> batchGroups = new HashMap();
        batchGroups.put(streamName, batchGroup);
        markBatchGroups(id, batchGroups);
        SpoutComponent c = new SpoutComponent(spout, streamName, parallelism, batchGroup);
        _batchPerTupleSpouts.put(id, c);
        return new SpoutDeclarerImpl(c);
    }
    
    public SpoutDeclarer setSpout(String id, String streamName, String txStateId, IBatchSpout spout, Integer parallelism, String batchGroup) {
        return setSpout(id, streamName, txStateId, new BatchSpoutExecutor(spout), parallelism, batchGroup);
    }
    
    public SpoutDeclarer setSpout(String id, String streamName, String txStateId, ITridentSpout spout, Integer parallelism, String batchGroup) {
        Map<String, String> batchGroups = new HashMap();
        batchGroups.put(streamName, batchGroup);
        markBatchGroups(id, batchGroups);

        TransactionalSpoutComponent c = new TransactionalSpoutComponent(spout, streamName, parallelism, txStateId, batchGroup);
        _spouts.put(id, c);
        return new SpoutDeclarerImpl(c);
    }
    
    // map from stream name to batch id
    public BoltDeclarer setBolt(String id, ITridentBatchBolt bolt, Integer parallelism, Set<String> committerBatches, Map<String, String> batchGroups) {
        markBatchGroups(id, batchGroups);
        Component c = new Component(bolt, parallelism, committerBatches);
        _bolts.put(id, c);
        return new BoltDeclarerImpl(c);
        
    }
    
    String masterCoordinator(String batchGroup) {
        return "$mastercoord-" + batchGroup;
    }
    
    static final String SPOUT_COORD_PREFIX = "$spoutcoord-";
    
    public static String spoutCoordinator(String spoutId) {
        return SPOUT_COORD_PREFIX + spoutId;
    }
    
    public static String spoutIdFromCoordinatorId(String coordId) {
        return coordId.substring(SPOUT_COORD_PREFIX.length());
    }
    
    Map<GlobalStreamId, String> fleshOutStreamBatchIds(boolean includeCommitStream) {
        Map<GlobalStreamId, String> ret = new HashMap<GlobalStreamId, String>(_batchIds);
        Set<String> allBatches = new HashSet(_batchIds.values());
        for(String b: allBatches) {
            ret.put(new GlobalStreamId(masterCoordinator(b), MasterBatchCoordinator.BATCH_STREAM_ID), b);
            if(includeCommitStream) {
                ret.put(new GlobalStreamId(masterCoordinator(b), MasterBatchCoordinator.COMMIT_STREAM_ID), b);
            }
            // DO NOT include the success stream as part of the batch. it should not trigger coordination tuples,
            // and is just a metadata tuple to assist in cleanup, should not trigger batch tracking
        }
        
        for(String id: _spouts.keySet()) {
            TransactionalSpoutComponent c = _spouts.get(id);
            if(c.batchGroupId!=null) {
                ret.put(new GlobalStreamId(spoutCoordinator(id), MasterBatchCoordinator.BATCH_STREAM_ID), c.batchGroupId);
            }
        }

        //this takes care of setting up coord streams for spouts and bolts
        for(GlobalStreamId s: _batchIds.keySet()) {
            String b = _batchIds.get(s);
            ret.put(new GlobalStreamId(s.get_componentId(), TridentBoltExecutor.COORD_STREAM(b)), b);
        }
        
        return ret;
    }
    
    public StormTopology buildTopology() {        
        TopologyBuilder builder = new TopologyBuilder();
        Map<GlobalStreamId, String> batchIdsForSpouts = fleshOutStreamBatchIds(false);
        Map<GlobalStreamId, String> batchIdsForBolts = fleshOutStreamBatchIds(true);

        Map<String, List<String>> batchesToCommitIds = new HashMap<String, List<String>>();
        Map<String, List<ITridentSpout>> batchesToSpouts = new HashMap<String, List<ITridentSpout>>();
        
        for(String id: _spouts.keySet()) {
            TransactionalSpoutComponent c = _spouts.get(id);
            if(c.spout instanceof IRichSpout) {
                
                //TODO: wrap this to set the stream name
                builder.setSpout(id, (IRichSpout) c.spout, c.parallelism);
            } else {
                String batchGroup = c.batchGroupId;
                if(!batchesToCommitIds.containsKey(batchGroup)) {
                    batchesToCommitIds.put(batchGroup, new ArrayList<String>());
                }
                batchesToCommitIds.get(batchGroup).add(c.commitStateId);

                if(!batchesToSpouts.containsKey(batchGroup)) {
                    batchesToSpouts.put(batchGroup, new ArrayList<ITridentSpout>());
                }
                batchesToSpouts.get(batchGroup).add((ITridentSpout) c.spout);
                
                
                BoltDeclarer scd =
                      builder.setBolt(spoutCoordinator(id), new TridentSpoutCoordinator(c.commitStateId, (ITridentSpout) c.spout))
                        .globalGrouping(masterCoordinator(c.batchGroupId), MasterBatchCoordinator.BATCH_STREAM_ID)
                        .globalGrouping(masterCoordinator(c.batchGroupId), MasterBatchCoordinator.SUCCESS_STREAM_ID);
                
                for(Map m: c.componentConfs) {
                    scd.addConfigurations(m);
                }
                
                Map<String, TridentBoltExecutor.CoordSpec> specs = new HashMap();
                specs.put(c.batchGroupId, new CoordSpec());
                BoltDeclarer bd = builder.setBolt(id,
                        new TridentBoltExecutor(
                          new TridentSpoutExecutor(
                            c.commitStateId,
                            c.streamName,
                            ((ITridentSpout) c.spout)),
                            batchIdsForSpouts,
                            specs),
                        c.parallelism);
                bd.allGrouping(spoutCoordinator(id), MasterBatchCoordinator.BATCH_STREAM_ID);
                bd.allGrouping(masterCoordinator(batchGroup), MasterBatchCoordinator.SUCCESS_STREAM_ID);
                if(c.spout instanceof ICommitterTridentSpout) {
                    bd.allGrouping(masterCoordinator(batchGroup), MasterBatchCoordinator.COMMIT_STREAM_ID);
                }
                for(Map m: c.componentConfs) {
                    bd.addConfigurations(m);
                }
            }
        }
        
        for(String id: _batchPerTupleSpouts.keySet()) {
            SpoutComponent c = _batchPerTupleSpouts.get(id);
            SpoutDeclarer d = builder.setSpout(id, new RichSpoutBatchTriggerer((IRichSpout) c.spout, c.streamName, c.batchGroupId), c.parallelism);
            
            for(Map conf: c.componentConfs) {
                d.addConfigurations(conf);
            }
        }
        
        for(String batch: batchesToCommitIds.keySet()) {
            List<String> commitIds = batchesToCommitIds.get(batch);
            builder.setSpout(masterCoordinator(batch), new MasterBatchCoordinator(commitIds, batchesToSpouts.get(batch)));
        }
                
        for(String id: _bolts.keySet()) {
            Component c = _bolts.get(id);
            
            Map<String, CoordSpec> specs = new HashMap();
            
            for(GlobalStreamId s: getBoltSubscriptionStreams(id)) {
                String batch = batchIdsForBolts.get(s);
                if(!specs.containsKey(batch)) specs.put(batch, new CoordSpec());
                CoordSpec spec = specs.get(batch);
                CoordType ct;
                if(_batchPerTupleSpouts.containsKey(s.get_componentId())) {
                    ct = CoordType.single();
                } else {
                    ct = CoordType.all();
                }
                spec.coords.put(s.get_componentId(), ct);
            }
            
            for(String b: c.committerBatches) {
                specs.get(b).commitStream = new GlobalStreamId(masterCoordinator(b), MasterBatchCoordinator.COMMIT_STREAM_ID);
            }
            
            BoltDeclarer d = builder.setBolt(id, new TridentBoltExecutor(c.bolt, batchIdsForBolts, specs), c.parallelism);
            for(Map conf: c.componentConfs) {
                d.addConfigurations(conf);
            }
            
            for(InputDeclaration inputDecl: c.declarations) {
               inputDecl.declare(d);
            }
            
            Map<String, Set<String>> batchToComponents = getBoltBatchToComponentSubscriptions(id);
            for(String b: batchToComponents.keySet()) {
                for(String comp: batchToComponents.get(b)) {
                    d.directGrouping(comp, TridentBoltExecutor.COORD_STREAM(b));
                }
            }
            
            for(String b: c.committerBatches) {
                d.allGrouping(masterCoordinator(b), MasterBatchCoordinator.COMMIT_STREAM_ID);
            }
        }

        return builder.createTopology();
    }
    
    private void markBatchGroups(String component, Map<String, String> batchGroups) {
        for(String stream: batchGroups.keySet()) {
            _batchIds.put(new GlobalStreamId(component, stream), batchGroups.get(stream));
        }
    }
    
    
    private static class SpoutComponent {
        public Object spout;
        public Integer parallelism;
        public List<Map> componentConfs = new ArrayList<Map>();
        String batchGroupId;
        String streamName;
        
        public SpoutComponent(Object spout, String streamName, Integer parallelism, String batchGroupId) {
            this.spout = spout;
            this.streamName = streamName;
            this.parallelism = parallelism;
            this.batchGroupId = batchGroupId;
        }

        @Override
        public String toString() {
            return ToStringBuilder.reflectionToString(this);
        }
    }
    
    private static class TransactionalSpoutComponent extends SpoutComponent {
        public String commitStateId; 
        
        public TransactionalSpoutComponent(Object spout, String streamName, Integer parallelism, String commitStateId, String batchGroupId) {
            super(spout, streamName, parallelism, batchGroupId);
            this.commitStateId = commitStateId;
        }

        @Override
        public String toString() {
            return ToStringBuilder.reflectionToString(this, ToStringStyle.MULTI_LINE_STYLE);
        }        
    }    
    
    private static class Component {
        public ITridentBatchBolt bolt;
        public Integer parallelism;
        public List<InputDeclaration> declarations = new ArrayList<InputDeclaration>();
        public List<Map> componentConfs = new ArrayList<Map>();
        public Set<String> committerBatches;
        
        public Component(ITridentBatchBolt bolt, Integer parallelism,Set<String> committerBatches) {
            this.bolt = bolt;
            this.parallelism = parallelism;
            this.committerBatches = committerBatches;
        }

        @Override
        public String toString() {
            return ToStringBuilder.reflectionToString(this, ToStringStyle.MULTI_LINE_STYLE);
        }        
    }
    
    Map<String, Set<String>> getBoltBatchToComponentSubscriptions(String id) {
        Map<String, Set<String>> ret = new HashMap();
        for(GlobalStreamId s: getBoltSubscriptionStreams(id)) {
            String b = _batchIds.get(s);
            if(!ret.containsKey(b)) ret.put(b, new HashSet());
            ret.get(b).add(s.get_componentId());
        }
        return ret;
    }
    
    List<GlobalStreamId> getBoltSubscriptionStreams(String id) {
        List<GlobalStreamId> ret = new ArrayList();
        Component c = _bolts.get(id);
        for(InputDeclaration d: c.declarations) {
            ret.add(new GlobalStreamId(d.getComponent(), d.getStream()));
        }
        return ret;
    }
    
    private static interface InputDeclaration {
        void declare(InputDeclarer declarer);
        String getComponent();
        String getStream();
    }
    
    private class SpoutDeclarerImpl extends BaseConfigurationDeclarer<SpoutDeclarer> implements SpoutDeclarer {
        SpoutComponent _component;
        
        public SpoutDeclarerImpl(SpoutComponent component) {
            _component = component;
        }
        
        @Override
        public SpoutDeclarer addConfigurations(Map conf) {
            _component.componentConfs.add(conf);
            return this;
        }        
    }
    
    private class BoltDeclarerImpl extends BaseConfigurationDeclarer<BoltDeclarer> implements BoltDeclarer {
        Component _component;
        
        public BoltDeclarerImpl(Component component) {
            _component = component;
        }
        
        @Override
        public BoltDeclarer fieldsGrouping(final String component, final Fields fields) {
            addDeclaration(new InputDeclaration() {
                @Override
                public void declare(InputDeclarer declarer) {
                    declarer.fieldsGrouping(component, fields);
                }

                @Override
                public String getComponent() {
                    return component;
                }

                @Override
                public String getStream() {
                    return null;
                }
            });
            return this;
        }

        @Override
        public BoltDeclarer fieldsGrouping(final String component, final String streamId, final Fields fields) {
            addDeclaration(new InputDeclaration() {
                @Override
                public void declare(InputDeclarer declarer) {
                    declarer.fieldsGrouping(component, streamId, fields);
                }                

                @Override
                public String getComponent() {
                    return component;
                }

                @Override
                public String getStream() {
                    return streamId;
                }
            });
            return this;
        }

        @Override
        public BoltDeclarer globalGrouping(final String component) {
            addDeclaration(new InputDeclaration() {
                @Override
                public void declare(InputDeclarer declarer) {
                    declarer.globalGrouping(component);
                }                

                @Override
                public String getComponent() {
                    return component;
                }
                
                @Override
                public String getStream() {
                    return null;
                }
            });
            return this;
        }

        @Override
        public BoltDeclarer globalGrouping(final String component, final String streamId) {
            addDeclaration(new InputDeclaration() {
                @Override
                public void declare(InputDeclarer declarer) {
                    declarer.globalGrouping(component, streamId);
                }                

                @Override
                public String getComponent() {
                    return component;
                }                

                @Override
                public String getStream() {
                    return streamId;
                }
            });
            return this;
        }

        @Override
        public BoltDeclarer shuffleGrouping(final String component) {
            addDeclaration(new InputDeclaration() {
                @Override
                public void declare(InputDeclarer declarer) {
                    declarer.shuffleGrouping(component);
                }                

                @Override
                public String getComponent() {
                    return component;
                }                

                @Override
                public String getStream() {
                    return null;
                }
            });
            return this;
        }

        @Override
        public BoltDeclarer shuffleGrouping(final String component, final String streamId) {
            addDeclaration(new InputDeclaration() {
                @Override
                public void declare(InputDeclarer declarer) {
                    declarer.shuffleGrouping(component, streamId);
                }                

                @Override
                public String getComponent() {
                    return component;
                }                

                @Override
                public String getStream() {
                    return streamId;
                }
            });
            return this;
        }

        @Override
        public BoltDeclarer localOrShuffleGrouping(final String component) {
            addDeclaration(new InputDeclaration() {
                @Override
                public void declare(InputDeclarer declarer) {
                    declarer.localOrShuffleGrouping(component);
                }                

                @Override
                public String getComponent() {
                    return component;
                }                

                @Override
                public String getStream() {
                    return null;
                }
            });
            return this;
        }

        @Override
        public BoltDeclarer localOrShuffleGrouping(final String component, final String streamId) {
            addDeclaration(new InputDeclaration() {
                @Override
                public void declare(InputDeclarer declarer) {
                    declarer.localOrShuffleGrouping(component, streamId);
                }                

                @Override
                public String getComponent() {
                    return component;
                }                

                @Override
                public String getStream() {
                    return streamId;
                }
            });
            return this;
        }
        
        @Override
        public BoltDeclarer noneGrouping(final String component) {
            addDeclaration(new InputDeclaration() {
                @Override
                public void declare(InputDeclarer declarer) {
                    declarer.noneGrouping(component);
                }                

                @Override
                public String getComponent() {
                    return component;
                }                

                @Override
                public String getStream() {
                    return null;
                }
            });
            return this;
        }

        @Override
        public BoltDeclarer noneGrouping(final String component, final String streamId) {
            addDeclaration(new InputDeclaration() {
                @Override
                public void declare(InputDeclarer declarer) {
                    declarer.noneGrouping(component, streamId);
                }                

                @Override
                public String getComponent() {
                    return component;
                }                

                @Override
                public String getStream() {
                    return streamId;
                }
            });
            return this;
        }

        @Override
        public BoltDeclarer allGrouping(final String component) {
            addDeclaration(new InputDeclaration() {
                @Override
                public void declare(InputDeclarer declarer) {
                    declarer.allGrouping(component);
                }                

                @Override
                public String getComponent() {
                    return component;
                }                

                @Override
                public String getStream() {
                    return null;
                }
            });
            return this;
        }

        @Override
        public BoltDeclarer allGrouping(final String component, final String streamId) {
            addDeclaration(new InputDeclaration() {
                @Override
                public void declare(InputDeclarer declarer) {
                    declarer.allGrouping(component, streamId);
                }                

                @Override
                public String getComponent() {
                    return component;
                }                

                @Override
                public String getStream() {
                    return streamId;
                }
            });
            return this;
        }

        @Override
        public BoltDeclarer directGrouping(final String component) {
            addDeclaration(new InputDeclaration() {
                @Override
                public void declare(InputDeclarer declarer) {
                    declarer.directGrouping(component);
                }                

                @Override
                public String getComponent() {
                    return component;
                }                

                @Override
                public String getStream() {
                    return null;
                }
            });
            return this;
        }

        @Override
        public BoltDeclarer directGrouping(final String component, final String streamId) {
            addDeclaration(new InputDeclaration() {
                @Override
                public void declare(InputDeclarer declarer) {
                    declarer.directGrouping(component, streamId);
                }                

                @Override
                public String getComponent() {
                    return component;
                }                

                @Override
                public String getStream() {
                    return streamId;
                }
            });
            return this;
        }
        
        @Override
        public BoltDeclarer customGrouping(final String component, final CustomStreamGrouping grouping) {
            addDeclaration(new InputDeclaration() {
                @Override
                public void declare(InputDeclarer declarer) {
                    declarer.customGrouping(component, grouping);
                }                

                @Override
                public String getComponent() {
                    return component;
                }                

                @Override
                public String getStream() {
                    return null;
                }
            });
            return this;        
        }

        @Override
        public BoltDeclarer customGrouping(final String component, final String streamId, final CustomStreamGrouping grouping) {
            addDeclaration(new InputDeclaration() {
                @Override
                public void declare(InputDeclarer declarer) {
                    declarer.customGrouping(component, streamId, grouping);
                }                

                @Override
                public String getComponent() {
                    return component;
                }                

                @Override
                public String getStream() {
                    return streamId;
                }
            });
            return this;
        }

        @Override
        public BoltDeclarer grouping(final GlobalStreamId stream, final Grouping grouping) {
            addDeclaration(new InputDeclaration() {
                @Override
                public void declare(InputDeclarer declarer) {
                    declarer.grouping(stream, grouping);
                }                

                @Override
                public String getComponent() {
                    return stream.get_componentId();
                }                

                @Override
                public String getStream() {
                    return stream.get_streamId();
                }
            });
            return this;
        }
        
        private void addDeclaration(InputDeclaration declaration) {
            _component.declarations.add(declaration);
        }

        @Override
        public BoltDeclarer addConfigurations(Map conf) {
            _component.componentConfs.add(conf);
            return this;
        }
    }    
}
