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
package org.apache.storm.drpc;

import org.apache.storm.Constants;
import org.apache.storm.ILocalDRPC;
import org.apache.storm.coordination.BatchBoltExecutor;
import org.apache.storm.coordination.CoordinatedBolt;
import org.apache.storm.coordination.CoordinatedBolt.FinishedCallback;
import org.apache.storm.coordination.CoordinatedBolt.IdStreamSpec;
import org.apache.storm.coordination.CoordinatedBolt.SourceArgs;
import org.apache.storm.coordination.IBatchBolt;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.generated.StreamInfo;
import org.apache.storm.grouping.CustomStreamGrouping;
import org.apache.storm.grouping.PartialKeyGrouping;
import org.apache.storm.topology.BaseConfigurationDeclarer;
import org.apache.storm.topology.BasicBoltExecutor;
import org.apache.storm.topology.BoltDeclarer;
import org.apache.storm.topology.IBasicBolt;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.InputDeclarer;
import org.apache.storm.topology.OutputFieldsGetter;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


// Trident subsumes the functionality provided by this class, so it's deprecated
@Deprecated
public class LinearDRPCTopologyBuilder {    
    String _function;
    List<Component> _components = new ArrayList<Component>();
    
    
    public LinearDRPCTopologyBuilder(String function) {
        _function = function;
    }
        
    public LinearDRPCInputDeclarer addBolt(IBatchBolt bolt, Number parallelism) {
        return addBolt(new BatchBoltExecutor(bolt), parallelism);
    }
    
    public LinearDRPCInputDeclarer addBolt(IBatchBolt bolt) {
        return addBolt(bolt, 1);
    }
    
    @Deprecated
    public LinearDRPCInputDeclarer addBolt(IRichBolt bolt, Number parallelism) {
        if(parallelism==null) parallelism = 1; 
        Component component = new Component(bolt, parallelism.intValue());
        _components.add(component);
        return new InputDeclarerImpl(component);
    }
    
    @Deprecated
    public LinearDRPCInputDeclarer addBolt(IRichBolt bolt) {
        return addBolt(bolt, null);
    }
    
    public LinearDRPCInputDeclarer addBolt(IBasicBolt bolt, Number parallelism) {
        return addBolt(new BasicBoltExecutor(bolt), parallelism);
    }

    public LinearDRPCInputDeclarer addBolt(IBasicBolt bolt) {
        return addBolt(bolt, null);
    }
        
    public StormTopology createLocalTopology(ILocalDRPC drpc) {
        return createTopology(new DRPCSpout(_function, drpc));
    }
    
    public StormTopology createRemoteTopology() {
        return createTopology(new DRPCSpout(_function));
    }
    
    
    private StormTopology createTopology(DRPCSpout spout) {
        final String SPOUT_ID = "spout";
        final String PREPARE_ID = "prepare-request";
        
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout(SPOUT_ID, spout);
        builder.setBolt(PREPARE_ID, new PrepareRequest())
                .noneGrouping(SPOUT_ID);
        int i=0;
        for(; i<_components.size();i++) {
            Component component = _components.get(i);
            
            Map<String, SourceArgs> source = new HashMap<String, SourceArgs>();
            if (i==1) {
                source.put(boltId(i-1), SourceArgs.single());
            } else if (i>=2) {
                source.put(boltId(i-1), SourceArgs.all());
            }
            IdStreamSpec idSpec = null;
            if(i==_components.size()-1 && component.bolt instanceof FinishedCallback) {
                idSpec = IdStreamSpec.makeDetectSpec(PREPARE_ID, PrepareRequest.ID_STREAM);
            }
            BoltDeclarer declarer = builder.setBolt(
                    boltId(i),
                    new CoordinatedBolt(component.bolt, source, idSpec),
                    component.parallelism);
            
            for(Map<String, Object> conf: component.componentConfs) {
                declarer.addConfigurations(conf);
            }
            
            if(idSpec!=null) {
                declarer.fieldsGrouping(idSpec.getGlobalStreamId().get_componentId(), PrepareRequest.ID_STREAM, new Fields("request"));
            }
            if(i==0 && component.declarations.isEmpty()) {
                declarer.noneGrouping(PREPARE_ID, PrepareRequest.ARGS_STREAM);
            } else {
                String prevId;
                if(i==0) {
                    prevId = PREPARE_ID;
                } else {
                    prevId = boltId(i-1);
                }
                for(InputDeclaration declaration: component.declarations) {
                    declaration.declare(prevId, declarer);
                }
            }
            if(i>0) {
                declarer.directGrouping(boltId(i-1), Constants.COORDINATED_STREAM_ID); 
            }
        }
        
        IRichBolt lastBolt = _components.get(_components.size()-1).bolt;
        OutputFieldsGetter getter = new OutputFieldsGetter();
        lastBolt.declareOutputFields(getter);
        Map<String, StreamInfo> streams = getter.getFieldsDeclaration();
        if(streams.size()!=1) {
            throw new RuntimeException("Must declare exactly one stream from last bolt in LinearDRPCTopology");
        }
        String outputStream = streams.keySet().iterator().next();
        List<String> fields = streams.get(outputStream).get_output_fields();
        if(fields.size()!=2) {
            throw new RuntimeException("Output stream of last component in LinearDRPCTopology must contain exactly two fields. The first should be the request id, and the second should be the result.");
        }

        builder.setBolt(boltId(i), new JoinResult(PREPARE_ID))
                .fieldsGrouping(boltId(i-1), outputStream, new Fields(fields.get(0)))
                .fieldsGrouping(PREPARE_ID, PrepareRequest.RETURN_STREAM, new Fields("request"));
        i++;
        builder.setBolt(boltId(i), new ReturnResults())
                .noneGrouping(boltId(i-1));
        return builder.createTopology();
    }
    
    private static String boltId(int index) {
        return "bolt" + index;
    }
    
    private static class Component {
        public IRichBolt bolt;
        public int parallelism;
        public List<Map<String, Object>> componentConfs = new ArrayList<>();
        public List<InputDeclaration> declarations = new ArrayList<InputDeclaration>();
        
        public Component(IRichBolt bolt, int parallelism) {
            this.bolt = bolt;
            this.parallelism = parallelism;
        }
    }
    
    private static interface InputDeclaration {
        public void declare(String prevComponent, InputDeclarer declarer);
    }
    
    private static class InputDeclarerImpl extends BaseConfigurationDeclarer<LinearDRPCInputDeclarer> implements LinearDRPCInputDeclarer {
        Component _component;
        
        public InputDeclarerImpl(Component component) {
            _component = component;
        }
        
        @Override
        public LinearDRPCInputDeclarer fieldsGrouping(final Fields fields) {
            addDeclaration(new InputDeclaration() {
                @Override
                public void declare(String prevComponent, InputDeclarer declarer) {
                    declarer.fieldsGrouping(prevComponent, fields);
                }                
            });
            return this;
        }

        @Override
        public LinearDRPCInputDeclarer fieldsGrouping(final String streamId, final Fields fields) {
            addDeclaration(new InputDeclaration() {
                @Override
                public void declare(String prevComponent, InputDeclarer declarer) {
                    declarer.fieldsGrouping(prevComponent, streamId, fields);
                }                
            });
            return this;
        }

        @Override
        public LinearDRPCInputDeclarer globalGrouping() {
            addDeclaration(new InputDeclaration() {
                @Override
                public void declare(String prevComponent, InputDeclarer declarer) {
                    declarer.globalGrouping(prevComponent);
                }                
            });
            return this;
        }

        @Override
        public LinearDRPCInputDeclarer globalGrouping(final String streamId) {
            addDeclaration(new InputDeclaration() {
                @Override
                public void declare(String prevComponent, InputDeclarer declarer) {
                    declarer.globalGrouping(prevComponent, streamId);
                }                
            });
            return this;
        }

        @Override
        public LinearDRPCInputDeclarer shuffleGrouping() {
            addDeclaration(new InputDeclaration() {
                @Override
                public void declare(String prevComponent, InputDeclarer declarer) {
                    declarer.shuffleGrouping(prevComponent);
                }                
            });
            return this;
        }

        @Override
        public LinearDRPCInputDeclarer shuffleGrouping(final String streamId) {
            addDeclaration(new InputDeclaration() {
                @Override
                public void declare(String prevComponent, InputDeclarer declarer) {
                    declarer.shuffleGrouping(prevComponent, streamId);
                }                
            });
            return this;
        }

        @Override
        public LinearDRPCInputDeclarer localOrShuffleGrouping() {
            addDeclaration(new InputDeclaration() {
                @Override
                public void declare(String prevComponent, InputDeclarer declarer) {
                    declarer.localOrShuffleGrouping(prevComponent);
                }                
            });
            return this;
        }

        @Override
        public LinearDRPCInputDeclarer localOrShuffleGrouping(final String streamId) {
            addDeclaration(new InputDeclaration() {
                @Override
                public void declare(String prevComponent, InputDeclarer declarer) {
                    declarer.localOrShuffleGrouping(prevComponent, streamId);
                }                
            });
            return this;
        }
        
        @Override
        public LinearDRPCInputDeclarer noneGrouping() {
            addDeclaration(new InputDeclaration() {
                @Override
                public void declare(String prevComponent, InputDeclarer declarer) {
                    declarer.noneGrouping(prevComponent);
                }                
            });
            return this;
        }

        @Override
        public LinearDRPCInputDeclarer noneGrouping(final String streamId) {
            addDeclaration(new InputDeclaration() {
                @Override
                public void declare(String prevComponent, InputDeclarer declarer) {
                    declarer.noneGrouping(prevComponent, streamId);
                }                
            });
            return this;
        }

        @Override
        public LinearDRPCInputDeclarer allGrouping() {
            addDeclaration(new InputDeclaration() {
                @Override
                public void declare(String prevComponent, InputDeclarer declarer) {
                    declarer.allGrouping(prevComponent);
                }                
            });
            return this;
        }

        @Override
        public LinearDRPCInputDeclarer allGrouping(final String streamId) {
            addDeclaration(new InputDeclaration() {
                @Override
                public void declare(String prevComponent, InputDeclarer declarer) {
                    declarer.allGrouping(prevComponent, streamId);
                }                
            });
            return this;
        }

        @Override
        public LinearDRPCInputDeclarer directGrouping() {
            addDeclaration(new InputDeclaration() {
                @Override
                public void declare(String prevComponent, InputDeclarer declarer) {
                    declarer.directGrouping(prevComponent);
                }                
            });
            return this;
        }

        @Override
        public LinearDRPCInputDeclarer directGrouping(final String streamId) {
            addDeclaration(new InputDeclaration() {
                @Override
                public void declare(String prevComponent, InputDeclarer declarer) {
                    declarer.directGrouping(prevComponent, streamId);
                }                
            });
            return this;
        }

        @Override
        public LinearDRPCInputDeclarer partialKeyGrouping(Fields fields) {
            return customGrouping(new PartialKeyGrouping(fields));
        }

        @Override
        public LinearDRPCInputDeclarer partialKeyGrouping(String streamId, Fields fields) {
            return customGrouping(streamId, new PartialKeyGrouping(fields));
        }

        @Override
        public LinearDRPCInputDeclarer customGrouping(final CustomStreamGrouping grouping) {
            addDeclaration(new InputDeclaration() {
                @Override
                public void declare(String prevComponent, InputDeclarer declarer) {
                    declarer.customGrouping(prevComponent, grouping);
                }                
            });
            return this;
        }

        @Override
        public LinearDRPCInputDeclarer customGrouping(final String streamId, final CustomStreamGrouping grouping) {
            addDeclaration(new InputDeclaration() {
                @Override
                public void declare(String prevComponent, InputDeclarer declarer) {
                    declarer.customGrouping(prevComponent, streamId, grouping);
                }                
            });
            return this;
        }
        
        private void addDeclaration(InputDeclaration declaration) {
            _component.declarations.add(declaration);
        }

        @Override
        public LinearDRPCInputDeclarer addConfigurations(Map<String, Object> conf) {
            _component.componentConfs.add(conf);
            return this;
        }
    }
}
