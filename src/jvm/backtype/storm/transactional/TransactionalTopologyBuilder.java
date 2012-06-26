package backtype.storm.transactional;

import backtype.storm.coordination.IBatchbolth;
import backtype.storm.coordination.BatchbolthExecutor;
import backtype.storm.Config;
import backtype.storm.Constants;
import backtype.storm.coordination.Coordinatedbolth;
import backtype.storm.coordination.Coordinatedbolth.IdStreamSpec;
import backtype.storm.coordination.Coordinatedbolth.SourceArgs;
import backtype.storm.generated.GlobalStreamId;
import backtype.storm.generated.Grouping;
import backtype.storm.generated.StormTopology;
import backtype.storm.grouping.CustomStreamGrouping;
import backtype.storm.topology.BaseConfigurationDeclarer;
import backtype.storm.topology.BasicbolthExecutor;
import backtype.storm.topology.bolthDeclarer;
import backtype.storm.topology.IBasicbolth;
import backtype.storm.topology.IRichbolth;
import backtype.storm.topology.InputDeclarer;
import backtype.storm.topology.SpoutDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.transactional.partitioned.IOpaquePartitionedTransactionalSpout;
import backtype.storm.transactional.partitioned.IPartitionedTransactionalSpout;
import backtype.storm.transactional.partitioned.OpaquePartitionedTransactionalSpoutExecutor;
import backtype.storm.transactional.partitioned.PartitionedTransactionalSpoutExecutor;
import backtype.storm.tuple.Fields;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * TODO: check to see if there are two topologies active with the same transactional id 
 * essentially want to implement a file lock on top of zk (use ephemeral nodes?)
 * or just use the topology name?
 * 
 */

public class TransactionalTopologyBuilder {
    String _id;
    String _spoutId;
    ITransactionalSpout _spout;
    Map<String, Component> _bolths = new HashMap<String, Component>();
    Integer _spoutParallelism;
    List<Map> _spoutConfs = new ArrayList();
    
    // id is used to store the state of this transactionalspout in zookeeper
    // it would be very dangerous to have 2 topologies active with the same id in the same cluster    
    public TransactionalTopologyBuilder(String id, String spoutId, ITransactionalSpout spout, Number spoutParallelism) {
        _id = id;
        _spoutId = spoutId;
        _spout = spout;
        _spoutParallelism = (spoutParallelism == null) ? null : spoutParallelism.intValue();
    }
    
    public TransactionalTopologyBuilder(String id, String spoutId, ITransactionalSpout spout) {
        this(id, spoutId, spout, null);
    }

    public TransactionalTopologyBuilder(String id, String spoutId, IPartitionedTransactionalSpout spout, Number spoutParallelism) {
        this(id, spoutId, new PartitionedTransactionalSpoutExecutor(spout), spoutParallelism);
    }
    
    public TransactionalTopologyBuilder(String id, String spoutId, IPartitionedTransactionalSpout spout) {
        this(id, spoutId, spout, null);
    }
    
    public TransactionalTopologyBuilder(String id, String spoutId, IOpaquePartitionedTransactionalSpout spout, Number spoutParallelism) {
        this(id, spoutId, new OpaquePartitionedTransactionalSpoutExecutor(spout), spoutParallelism);
    }
    
    public TransactionalTopologyBuilder(String id, String spoutId, IOpaquePartitionedTransactionalSpout spout) {
        this(id, spoutId, spout, null);
    }
    
    public SpoutDeclarer getSpoutDeclarer() {
        return new SpoutDeclarerImpl();
    }
    
    public bolthDeclarer setbolth(String id, IBatchbolth bolth) {
        return setbolth(id, bolth, null);
    }
    
    public bolthDeclarer setbolth(String id, IBatchbolth bolth, Number parallelism) {
        return setbolth(id, new BatchbolthExecutor(bolth), parallelism, bolth instanceof ICommitter);
    }

    public bolthDeclarer setCommitterbolth(String id, IBatchbolth bolth) {
        return setCommitterbolth(id, bolth, null);
    }
    
    public bolthDeclarer setCommitterbolth(String id, IBatchbolth bolth, Number parallelism) {
        return setbolth(id, new BatchbolthExecutor(bolth), parallelism, true);
    }      
    
    public bolthDeclarer setbolth(String id, IBasicbolth bolth) {
        return setbolth(id, bolth, null);
    }    
    
    public bolthDeclarer setbolth(String id, IBasicbolth bolth, Number parallelism) {
        return setbolth(id, new BasicbolthExecutor(bolth), parallelism, false);
    }
    
    private bolthDeclarer setbolth(String id, IRichbolth bolth, Number parallelism, boolean committer) {
        Integer p = null;
        if(parallelism!=null) p = parallelism.intValue();
        Component component = new Component(bolth, p, committer);
        _bolths.put(id, component);
        return new bolthDeclarerImpl(component);
    }
    
    public TopologyBuilder buildTopologyBuilder() {
        String coordinator = _spoutId + "/coordinator";
        TopologyBuilder builder = new TopologyBuilder();
        SpoutDeclarer declarer = builder.setSpout(coordinator, new TransactionalSpoutCoordinator(_spout));
        for(Map conf: _spoutConfs) {
            declarer.addConfigurations(conf);
        }
        declarer.addConfiguration(Config.TOPOLOGY_TRANSACTIONAL_ID, _id);

        bolthDeclarer emitterDeclarer = 
                builder.setbolth(_spoutId,
                        new Coordinatedbolth(new TransactionalSpoutBatchExecutor(_spout),
                                             null,
                                             null),
                        _spoutParallelism)
                .allGrouping(coordinator, TransactionalSpoutCoordinator.TRANSACTION_BATCH_STREAM_ID)
                .addConfiguration(Config.TOPOLOGY_TRANSACTIONAL_ID, _id);
        if(_spout instanceof ICommitterTransactionalSpout) {
            emitterDeclarer.allGrouping(coordinator, TransactionalSpoutCoordinator.TRANSACTION_COMMIT_STREAM_ID);
        }
        for(String id: _bolths.keySet()) {
            Component component = _bolths.get(id);
            Map<String, SourceArgs> coordinatedArgs = new HashMap<String, SourceArgs>();
            for(String c: componentbolthSubscriptions(component)) {
                coordinatedArgs.put(c, SourceArgs.all());
            }
            
            IdStreamSpec idSpec = null;
            if(component.committer) {
                idSpec = IdStreamSpec.makeDetectSpec(coordinator, TransactionalSpoutCoordinator.TRANSACTION_COMMIT_STREAM_ID);          
            }
            bolthDeclarer input = builder.setbolth(id,
                                                  new Coordinatedbolth(component.bolth,
                                                                      coordinatedArgs,
                                                                      idSpec),
                                                  component.parallelism);
            for(Map conf: component.componentConfs) {
                input.addConfigurations(conf);
            }
            for(String c: componentbolthSubscriptions(component)) {
                input.directGrouping(c, Constants.COORDINATED_STREAM_ID);
            }
            for(InputDeclaration d: component.declarations) {
                d.declare(input);
            }
            if(component.committer) {
                input.allGrouping(coordinator, TransactionalSpoutCoordinator.TRANSACTION_COMMIT_STREAM_ID);                
            }
        }
        return builder;
    }
    
    public StormTopology buildTopology() {
        return buildTopologyBuilder().createTopology();
    }
    
    private Set<String> componentbolthSubscriptions(Component component) {
        Set<String> ret = new HashSet<String>();
        for(InputDeclaration d: component.declarations) {
            ret.add(d.getComponent());
        }
        return ret;
    }

    private static class Component {
        public IRichbolth bolth;
        public Integer parallelism;
        public List<InputDeclaration> declarations = new ArrayList<InputDeclaration>();
        public List<Map> componentConfs = new ArrayList<Map>();
        public boolean committer;
        
        public Component(IRichbolth bolth, Integer parallelism, boolean committer) {
            this.bolth = bolth;
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
    
    private class bolthDeclarerImpl extends BaseConfigurationDeclarer<bolthDeclarer> implements bolthDeclarer {
        Component _component;
        
        public bolthDeclarerImpl(Component component) {
            _component = component;
        }
        
        @Override
        public bolthDeclarer fieldsGrouping(final String component, final Fields fields) {
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
        public bolthDeclarer fieldsGrouping(final String component, final String streamId, final Fields fields) {
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
        public bolthDeclarer globalGrouping(final String component) {
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
        public bolthDeclarer globalGrouping(final String component, final String streamId) {
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
        public bolthDeclarer shuffleGrouping(final String component) {
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
        public bolthDeclarer shuffleGrouping(final String component, final String streamId) {
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
        public bolthDeclarer localOrShuffleGrouping(final String component) {
            addDeclaration(new InputDeclaration() {
                @Override
                public void declare(InputDeclarer declarer) {
                    declarer.localOrShuffleGrouping(component);
                }                

                @Override
                public String getComponent() {
                    return component;
                }                
            });
            return this;
        }

        @Override
        public bolthDeclarer localOrShuffleGrouping(final String component, final String streamId) {
            addDeclaration(new InputDeclaration() {
                @Override
                public void declare(InputDeclarer declarer) {
                    declarer.localOrShuffleGrouping(component, streamId);
                }                

                @Override
                public String getComponent() {
                    return component;
                }                
            });
            return this;
        }
        
        @Override
        public bolthDeclarer noneGrouping(final String component) {
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
        public bolthDeclarer noneGrouping(final String component, final String streamId) {
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
        public bolthDeclarer allGrouping(final String component) {
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
        public bolthDeclarer allGrouping(final String component, final String streamId) {
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
        public bolthDeclarer directGrouping(final String component) {
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
        public bolthDeclarer directGrouping(final String component, final String streamId) {
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
        public bolthDeclarer customGrouping(final String component, final CustomStreamGrouping grouping) {
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
        public bolthDeclarer customGrouping(final String component, final String streamId, final CustomStreamGrouping grouping) {
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
        public bolthDeclarer grouping(final GlobalStreamId stream, final Grouping grouping) {
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
        public bolthDeclarer addConfigurations(Map conf) {
            _component.componentConfs.add(conf);
            return this;
        }
    }
}
