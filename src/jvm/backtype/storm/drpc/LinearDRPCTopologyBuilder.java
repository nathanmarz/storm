package backtype.storm.drpc;

import backtype.storm.Constants;
import backtype.storm.ILocalDRPC;
import backtype.storm.drpc.CoordinatedBolt.FinishedCallback;
import backtype.storm.drpc.CoordinatedBolt.SourceArgs;
import backtype.storm.generated.StormTopology;
import backtype.storm.generated.StreamInfo;
import backtype.storm.topology.BasicBoltExecutor;
import backtype.storm.topology.IBasicBolt;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.InputDeclarer;
import backtype.storm.topology.OutputFieldsGetter;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;


// need a "final bolt" method, that does fields groupings based on the first field of previous streams.
// preparerequest needs to emit to a special stream to indicate which task in the last bolt is responsible for that id?
// -- what if it's shuffle grouping all the way through? need to enforce that last bolt do fields grouping on id...
public class LinearDRPCTopologyBuilder {    
    String _function;
    List<Component> _components = new ArrayList<Component>();
    
    
    public LinearDRPCTopologyBuilder(String function) {
        _function = function;
    }
        
    public LinearDRPCInputDeclarer addBolt(IRichBolt bolt, int parallelism) {
        Component component = new Component(bolt, parallelism);
        _components.add(component);
        return new InputDeclarerImpl(component);
    }
    
    public LinearDRPCInputDeclarer addBolt(IRichBolt bolt) {
        return addBolt(bolt, 1);
    }
    
    public LinearDRPCInputDeclarer addBolt(IBasicBolt bolt, int parallelism) {
        return addBolt(new BasicBoltExecutor(bolt), parallelism);
    }

    public LinearDRPCInputDeclarer addBolt(IBasicBolt bolt) {
        return addBolt(bolt, 1);
    }
        
    public StormTopology createLocalTopology(ILocalDRPC drpc) {
        return createTopology(new DRPCSpout(_function, drpc));
    }
    
    public StormTopology createRemoteTopology() {
        return createTopology(new DRPCSpout(_function));
    }
    
    
    private StormTopology createTopology(DRPCSpout spout) {
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout(1, spout);
        builder.setBolt(2, new PrepareRequest())
                .noneGrouping(1);
        int id = 3;
        for(int i=0; i<_components.size();i++) {
            Component component = _components.get(i);
            
            SourceArgs source;
            if(i==0) {
                source = null;
            } else if (i==1) {
                source = SourceArgs.single();
            } else {
                source = SourceArgs.all();
            }
            Integer idComponent = null;
            if(i==_components.size()-1 && component.bolt instanceof FinishedCallback) {
                idComponent = 2;
            }
            InputDeclarer declarer = builder.setBolt(
                    id,
                    new CoordinatedBolt(component.bolt, source, idComponent),
                    component.parallelism);
            if(idComponent!=null) {
                declarer.fieldsGrouping(idComponent, PrepareRequest.ID_STREAM, new Fields("request"));
            }
            if(i==0 && component.declarations.size()==0) {
                declarer.noneGrouping(2, PrepareRequest.ARGS_STREAM);
            } else {
                for(InputDeclaration declaration: component.declarations) {
                    declaration.declare(id-1, declarer);
                }
            }
            if(i>0) {
                declarer.directGrouping(id-1, Constants.COORDINATED_STREAM_ID); 
            }
            id++;
        }
        
        IRichBolt lastBolt = _components.get(_components.size()-1).bolt;
        OutputFieldsGetter getter = new OutputFieldsGetter();
        lastBolt.declareOutputFields(getter);
        Map<Integer, StreamInfo> streams = getter.getFieldsDeclaration();
        if(streams.size()!=1) {
            throw new RuntimeException("Must declare exactly one stream from last bolt in LinearDRPCTopology");
        }
        int outputStream = streams.keySet().iterator().next();
        List<String> fields = streams.get(outputStream).get_output_fields();
        if(fields.size()!=2) {
            throw new RuntimeException("Output stream of last component in LinearDRPCTopology must contain exactly two fields. The first should be the request id, and the second should be the result.");
        }

        builder.setBolt(id, new JoinResult(2))
                .fieldsGrouping(id-1, outputStream, new Fields(fields.get(0)))
                .fieldsGrouping(2, PrepareRequest.RETURN_STREAM, new Fields("request"));
        id++;
        builder.setBolt(id, new ReturnResults())
                .noneGrouping(id-1);
        return builder.createTopology();
    }
    
    private static class Component {
        public IRichBolt bolt;
        public int parallelism;
        public List<InputDeclaration> declarations = new ArrayList<InputDeclaration>();
        
        public Component(IRichBolt bolt, int parallelism) {
            this.bolt = bolt;
            this.parallelism = parallelism;
        }
    }
    
    private static interface InputDeclaration {
        public void declare(int prevComponent, InputDeclarer declarer);
    }
    
    private class InputDeclarerImpl implements LinearDRPCInputDeclarer {
        Component _component;
        
        public InputDeclarerImpl(Component component) {
            _component = component;
        }
        
        @Override
        public LinearDRPCInputDeclarer fieldsGrouping(final Fields fields) {
            addDeclaration(new InputDeclaration() {
                @Override
                public void declare(int prevComponent, InputDeclarer declarer) {
                    declarer.fieldsGrouping(prevComponent, fields);
                }                
            });
            return this;
        }

        @Override
        public LinearDRPCInputDeclarer fieldsGrouping(final int streamId, final Fields fields) {
            addDeclaration(new InputDeclaration() {
                @Override
                public void declare(int prevComponent, InputDeclarer declarer) {
                    declarer.fieldsGrouping(prevComponent, streamId, fields);
                }                
            });
            return this;
        }

        @Override
        public LinearDRPCInputDeclarer globalGrouping() {
            addDeclaration(new InputDeclaration() {
                @Override
                public void declare(int prevComponent, InputDeclarer declarer) {
                    declarer.globalGrouping(prevComponent);
                }                
            });
            return this;
        }

        @Override
        public LinearDRPCInputDeclarer globalGrouping(final int streamId) {
            addDeclaration(new InputDeclaration() {
                @Override
                public void declare(int prevComponent, InputDeclarer declarer) {
                    declarer.globalGrouping(prevComponent, streamId);
                }                
            });
            return this;
        }

        @Override
        public LinearDRPCInputDeclarer shuffleGrouping() {
            addDeclaration(new InputDeclaration() {
                @Override
                public void declare(int prevComponent, InputDeclarer declarer) {
                    declarer.shuffleGrouping(prevComponent);
                }                
            });
            return this;
        }

        @Override
        public LinearDRPCInputDeclarer shuffleGrouping(final int streamId) {
            addDeclaration(new InputDeclaration() {
                @Override
                public void declare(int prevComponent, InputDeclarer declarer) {
                    declarer.shuffleGrouping(prevComponent, streamId);
                }                
            });
            return this;
        }

        @Override
        public LinearDRPCInputDeclarer noneGrouping() {
            addDeclaration(new InputDeclaration() {
                @Override
                public void declare(int prevComponent, InputDeclarer declarer) {
                    declarer.noneGrouping(prevComponent);
                }                
            });
            return this;
        }

        @Override
        public LinearDRPCInputDeclarer noneGrouping(final int streamId) {
            addDeclaration(new InputDeclaration() {
                @Override
                public void declare(int prevComponent, InputDeclarer declarer) {
                    declarer.noneGrouping(prevComponent, streamId);
                }                
            });
            return this;
        }

        @Override
        public LinearDRPCInputDeclarer allGrouping() {
            addDeclaration(new InputDeclaration() {
                @Override
                public void declare(int prevComponent, InputDeclarer declarer) {
                    declarer.allGrouping(prevComponent);
                }                
            });
            return this;
        }

        @Override
        public LinearDRPCInputDeclarer allGrouping(final int streamId) {
            addDeclaration(new InputDeclaration() {
                @Override
                public void declare(int prevComponent, InputDeclarer declarer) {
                    declarer.allGrouping(prevComponent, streamId);
                }                
            });
            return this;
        }

        @Override
        public LinearDRPCInputDeclarer directGrouping() {
            addDeclaration(new InputDeclaration() {
                @Override
                public void declare(int prevComponent, InputDeclarer declarer) {
                    declarer.directGrouping(prevComponent);
                }                
            });
            return this;
        }

        @Override
        public LinearDRPCInputDeclarer directGrouping(final int streamId) {
            addDeclaration(new InputDeclaration() {
                @Override
                public void declare(int prevComponent, InputDeclarer declarer) {
                    declarer.directGrouping(prevComponent, streamId);
                }                
            });
            return this;
        }
        
        private void addDeclaration(InputDeclaration declaration) {
            _component.declarations.add(declaration);
        }        
    }
}
