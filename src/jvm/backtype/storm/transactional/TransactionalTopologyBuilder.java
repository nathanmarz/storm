package backtype.storm.transactional;

import backtype.storm.Constants;
import backtype.storm.coordination.CoordinatedBolt;
import backtype.storm.coordination.CoordinatedBolt.SourceArgs;
import backtype.storm.generated.GlobalStreamId;
import backtype.storm.generated.Grouping;
import backtype.storm.generated.StormTopology;
import backtype.storm.grouping.CustomStreamGrouping;
import backtype.storm.topology.BaseConfigurationDeclarer;
import backtype.storm.topology.BasicBoltExecutor;
import backtype.storm.topology.BoltDeclarer;
import backtype.storm.topology.IBasicBolt;
import backtype.storm.topology.IRichBolt;
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

public class TransactionalTopologyBuilder {
    String _spoutId;
    ITransactionalSpout _spout;
    Map<String, Component> _bolts;
    Integer _spoutParallelism;
    List<Map> _spoutConfs = new ArrayList();
    
    
    public TransactionalTopologyBuilder(String spoutId, ITransactionalSpout spout, Integer spoutParallelism) {
        _spoutId = spoutId;
        _spout = spout;
        _spoutParallelism = spoutParallelism;
    }
    
    public SpoutDeclarer getSpoutDeclarer() {
        return new SpoutDeclarerImpl();
    }
    
    public BoltDeclarer setBolt(String id, ITransactionalBolt bolt) {
        return setBolt(id, bolt, null);
    }
    
    public BoltDeclarer setBolt(String id, ITransactionalBolt bolt, Integer parallelism) {
        return setBolt(id, new TransactionalBoltExecutor(bolt), parallelism, bolt instanceof ICommittable);
    }

    public BoltDeclarer setBolt(String id, IBasicBolt bolt) {
        return setBolt(id, bolt, null);
    }    
    
    public BoltDeclarer setBolt(String id, IBasicBolt bolt, Integer parallelism) {
        return setBolt(id, new BasicBoltExecutor(bolt), parallelism, false);
    }
    
    private BoltDeclarer setBolt(String id, IRichBolt bolt, Integer parallelism, boolean committer) {
        Component component = new Component(bolt, parallelism, committer);
        _bolts.put(id, component);
        return new BoltDeclarerImpl(component);
    }
    
    public StormTopology buildTopology() {
        String coordinator = _spoutId + "/coordinator";
        TopologyBuilder builder = new TopologyBuilder();
        SpoutDeclarer declarer = builder.setSpout(coordinator, new TransactionalSpoutCoordinator(_spout));
        for(Map conf: _spoutConfs) {
            declarer.addConfigurations(conf);
        }
        builder.setBolt(_spoutId,
                        new CoordinatedBolt(new TransactionalSpoutBatchExecutor(_spout),
                                             coordinator,
                                             null,
                                             null),
                        _spoutParallelism)
                .allGrouping(coordinator, TransactionalSpoutCoordinator.TRANSACTION_BATCH_STREAM_ID);
        for(String id: _bolts.keySet()) {
            Component component = _bolts.get(id);
            Map<String, SourceArgs> coordinatedArgs = new HashMap<String, SourceArgs>();
            for(String c: componentBoltSubscriptions(component)) {
                coordinatedArgs.put(c, SourceArgs.all());
            }
            BoltDeclarer input = builder.setBolt(id,
                                                  new CoordinatedBolt(component.bolt,
                                                                      coordinatedArgs,
                                                                      null),
                                                  component.parallelism);
            for(Map conf: component.componentConfs) {
                input.addConfigurations(conf);
            }
            for(String c: componentBoltSubscriptions(component)) {
                input.directGrouping(c, Constants.COORDINATED_STREAM_ID);
            }
            for(InputDeclaration d: component.declarations) {
                d.declare(input);
            }
            if(component.committer) {
                input.allGrouping(coordinator, TransactionalSpoutCoordinator.TRANSACTION_COMMIT_STREAM_ID);
                
                // this is to guarantee that they create the transactionalbolt for every attempt
                input.allGrouping(coordinator, TransactionalSpoutCoordinator.TRANSACTION_BATCH_STREAM_ID);
            }
        }
        return builder.createTopology();
    }
    
    private Set<String> componentBoltSubscriptions(Component component) {
        Set<String> ret = new HashSet<String>();
        for(InputDeclaration d: component.declarations) {
            ret.add(d.getComponent());
        }
        return ret;
    }

    private static class Component {
        public IRichBolt bolt;
        public int parallelism;
        public List<InputDeclaration> declarations = new ArrayList<InputDeclaration>();
        public List<Map> componentConfs = new ArrayList<Map>();
        public boolean committer;
        
        public Component(IRichBolt bolt, int parallelism, boolean committer) {
            this.bolt = bolt;
            this.parallelism = parallelism;
            this.committer = committer;
        }
    }
    
    private static interface InputDeclaration {
        void declare(InputDeclarer declarer);
        String getComponent();
    }
    
    private class SpoutDeclarerImpl extends BaseConfigurationDeclarer<SpoutDeclarer> implements SpoutDeclarer {
        @Override
        public SpoutDeclarer addConfigurations(Map conf) {
            _spoutConfs.add(conf);
            return this;
        }        
    }
    
    private class BoltDeclarerImpl extends BaseConfigurationDeclarer<BoltDeclarer> implements BoltDeclarer {
        Component _component;
        
        public BoltDeclarerImpl(Component component) {
            _component = component;
        }
        
        @Override
        public InputDeclarer fieldsGrouping(final String component, final Fields fields) {
            addDeclaration(new InputDeclaration() {
                @Override
                public void declare(InputDeclarer declarer) {
                    declarer.fieldsGrouping(component, fields);
                }

                @Override
                public String getComponent() {
                    return component;
                }                
            });
            return this;
        }

        @Override
        public InputDeclarer fieldsGrouping(final String component, final String streamId, final Fields fields) {
            addDeclaration(new InputDeclaration() {
                @Override
                public void declare(InputDeclarer declarer) {
                    declarer.fieldsGrouping(component, streamId, fields);
                }                

                @Override
                public String getComponent() {
                    return component;
                }                
            });
            return this;
        }

        @Override
        public InputDeclarer globalGrouping(final String component) {
            addDeclaration(new InputDeclaration() {
                @Override
                public void declare(InputDeclarer declarer) {
                    declarer.globalGrouping(component);
                }                

                @Override
                public String getComponent() {
                    return component;
                }                
            });
            return this;
        }

        @Override
        public InputDeclarer globalGrouping(final String component, final String streamId) {
            addDeclaration(new InputDeclaration() {
                @Override
                public void declare(InputDeclarer declarer) {
                    declarer.globalGrouping(component, streamId);
                }                

                @Override
                public String getComponent() {
                    return component;
                }                
            });
            return this;
        }

        @Override
        public InputDeclarer shuffleGrouping(final String component) {
            addDeclaration(new InputDeclaration() {
                @Override
                public void declare(InputDeclarer declarer) {
                    declarer.shuffleGrouping(component);
                }                

                @Override
                public String getComponent() {
                    return component;
                }                
            });
            return this;
        }

        @Override
        public InputDeclarer shuffleGrouping(final String component, final String streamId) {
            addDeclaration(new InputDeclaration() {
                @Override
                public void declare(InputDeclarer declarer) {
                    declarer.shuffleGrouping(component, streamId);
                }                

                @Override
                public String getComponent() {
                    return component;
                }                
            });
            return this;
        }

        @Override
        public InputDeclarer noneGrouping(final String component) {
            addDeclaration(new InputDeclaration() {
                @Override
                public void declare(InputDeclarer declarer) {
                    declarer.noneGrouping(component);
                }                

                @Override
                public String getComponent() {
                    return component;
                }                
            });
            return this;
        }

        @Override
        public InputDeclarer noneGrouping(final String component, final String streamId) {
            addDeclaration(new InputDeclaration() {
                @Override
                public void declare(InputDeclarer declarer) {
                    declarer.noneGrouping(component, streamId);
                }                

                @Override
                public String getComponent() {
                    return component;
                }                
            });
            return this;
        }

        @Override
        public InputDeclarer allGrouping(final String component) {
            addDeclaration(new InputDeclaration() {
                @Override
                public void declare(InputDeclarer declarer) {
                    declarer.allGrouping(component);
                }                

                @Override
                public String getComponent() {
                    return component;
                }                
            });
            return this;
        }

        @Override
        public InputDeclarer allGrouping(final String component, final String streamId) {
            addDeclaration(new InputDeclaration() {
                @Override
                public void declare(InputDeclarer declarer) {
                    declarer.allGrouping(component, streamId);
                }                

                @Override
                public String getComponent() {
                    return component;
                }                
            });
            return this;
        }

        @Override
        public InputDeclarer directGrouping(final String component) {
            addDeclaration(new InputDeclaration() {
                @Override
                public void declare(InputDeclarer declarer) {
                    declarer.directGrouping(component);
                }                

                @Override
                public String getComponent() {
                    return component;
                }                
            });
            return this;
        }

        @Override
        public InputDeclarer directGrouping(final String component, final String streamId) {
            addDeclaration(new InputDeclaration() {
                @Override
                public void declare(InputDeclarer declarer) {
                    declarer.directGrouping(component, streamId);
                }                

                @Override
                public String getComponent() {
                    return component;
                }                
            });
            return this;
        }
        
        @Override
        public InputDeclarer customGrouping(final String component, final CustomStreamGrouping grouping) {
            addDeclaration(new InputDeclaration() {
                @Override
                public void declare(InputDeclarer declarer) {
                    declarer.customGrouping(component, grouping);
                }                

                @Override
                public String getComponent() {
                    return component;
                }                
            });
            return this;        
        }

        @Override
        public InputDeclarer customGrouping(final String component, final String streamId, final CustomStreamGrouping grouping) {
            addDeclaration(new InputDeclaration() {
                @Override
                public void declare(InputDeclarer declarer) {
                    declarer.customGrouping(component, streamId, grouping);
                }                

                @Override
                public String getComponent() {
                    return component;
                }                
            });
            return this;
        }

        @Override
        public InputDeclarer grouping(final GlobalStreamId stream, final Grouping grouping) {
            addDeclaration(new InputDeclaration() {
                @Override
                public void declare(InputDeclarer declarer) {
                    declarer.grouping(stream, grouping);
                }                

                @Override
                public String getComponent() {
                    return stream.get_componentId();
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
