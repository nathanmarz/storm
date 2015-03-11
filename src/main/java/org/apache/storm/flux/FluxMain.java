package org.apache.storm.flux;


import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.generated.StormTopology;
import backtype.storm.topology.*;
import backtype.storm.tuple.Fields;
import backtype.storm.utils.Utils;
import org.apache.storm.flux.model.*;
import org.apache.storm.flux.parser.FluxParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Array;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class FluxMain {
    private static Logger LOG = LoggerFactory.getLogger(FluxMain.class);

    /**
     *
     * @param args
     * @throws Exception
     */
    public static void main(String[] args) throws Exception {
        // TODO parse args, and run local or remote

        TopologyDef topologyDef = FluxParser.parse(args[0]);

        // merge contents of `config` into topology config
        Config conf = buildConfig(topologyDef);
        StormTopology topology = buildTopology(topologyDef);


        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("test", conf, topology);
        Utils.sleep(60000);
        cluster.killTopology("test");
        cluster.shutdown();

    }

    /**
     * Given a topology definition, return a populated `backtype.storm.Config` instance.
     *
     * @param topologyDef
     * @return
     */
    public static Config buildConfig(TopologyDef topologyDef){
        // merge contents of `config` into topology config
        Config conf = new Config();
        conf.putAll(topologyDef.getConfig());
        return conf;
    }

    /**
     * Given a topology definition, return a Storm topology that can be run either locally or remotely.
     *
     * @param topologyDef
     * @return
     * @throws IllegalAccessException
     * @throws InstantiationException
     * @throws ClassNotFoundException
     * @throws NoSuchMethodException
     * @throws InvocationTargetException
     */
    public static StormTopology buildTopology(TopologyDef topologyDef) throws IllegalAccessException, InstantiationException, ClassNotFoundException, NoSuchMethodException, InvocationTargetException {
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


            switch(grouping.getType()) {
                case SHUFFLE:
                    declarer.shuffleGrouping(stream.getFrom(), streamId);
                    break;
                case FIELDS:
                    //TODO check for null grouping args
                    declarer.fieldsGrouping(stream.getFrom(), streamId, new Fields(grouping.getArgs()));
                    break;
                case ALL:
                    declarer.allGrouping(stream.getFrom(), streamId);
                    break;
                case DIRECT:
                    declarer.directGrouping(stream.getFrom(), streamId);
                    break;
                case GLOBAL:
                    declarer.globalGrouping(stream.getFrom(), streamId);
                    break;
                case LOCAL_OR_SHUFFLE:
                    declarer.localOrShuffleGrouping(stream.getFrom(), streamId);
                    break;
                case NONE:
                    declarer.noneGrouping(stream.getFrom(), streamId);
                    break;
                // TODO custom groupings
                default:
                    throw new UnsupportedOperationException("unsupported grouping type: " + grouping);
            }
        }
        return builder.createTopology();
    }


    /**
     * Given a spout definition, return a Storm spout implementation by attempting to find a matching constructor
     * in the given spout class. Perform list to array conversion as necessary.
     * @param def
     * @return
     * @throws ClassNotFoundException
     * @throws IllegalAccessException
     * @throws InstantiationException
     * @throws NoSuchMethodException
     * @throws InvocationTargetException
     */
    private static IRichSpout buildSpout(SpoutDef def) throws ClassNotFoundException, IllegalAccessException, InstantiationException, NoSuchMethodException, InvocationTargetException {
        Class clazz = Class.forName(def.getClassName());
        IRichSpout spout = null;
        if(def.hasConstructorArgs()){
            LOG.info("Found constructor arguments in definition: " + def.getConstructorArgs().getClass().getName());
            Constructor con = findCompatibleConstructor(def.getConstructorArgs(), clazz);
            if(con != null){
                LOG.info("Found something seemingly compatible, attempting invocation...");
                spout = (IRichSpout) con.newInstance(getConstructorArgsWithListCoercian(def.getConstructorArgs(), con));
            } else {
                throw new IllegalArgumentException("Couldn't find a suitable Spout constructor.");
            }
        } else {
            spout = (IRichSpout) clazz.newInstance();
        }
        return spout;
    }


    /**
     * Given a list of bolt definitions, build a map of Storm bolts with the bolt definition id as the key.
     * Attempt to coerce the given constructor arguments to a matching bolt constructor as much as possible.
     * @param boltDefs
     * @return
     * @throws ClassNotFoundException
     * @throws IllegalAccessException
     * @throws InstantiationException
     * @throws NoSuchMethodException
     * @throws InvocationTargetException
     */
    private static Map<String, Object> buildBoltMap(List<BoltDef> boltDefs) throws ClassNotFoundException, IllegalAccessException, InstantiationException, NoSuchMethodException, InvocationTargetException {
        Map<String, Object> retval= new HashMap<String, Object>();
        for(BoltDef def : boltDefs){

            Class clazz = Class.forName(def.getClassName());
            Object bolt = null;
            LOG.info("Attempting to instantiate bolt: {}", def.getClassName());
            if(def.hasConstructorArgs()){
                LOG.info("Found constructor arguments in definition: " + def.getConstructorArgs().getClass().getName());
                Constructor con = findCompatibleConstructor(def.getConstructorArgs(), clazz);
                if(con != null){
                    LOG.info("Found something seemingly compatible, attempting invocation...");
                    bolt = con.newInstance(getConstructorArgsWithListCoercian(def.getConstructorArgs(), con));
                } else {
                    throw new IllegalArgumentException("Couldn't find a suitable Spout constructor.");
                }
            } else {
                bolt = clazz.newInstance();
            }
            retval.put(def.getId(), bolt);
        }
        return retval;
    }

    /**
     * Given a list of constructor arguments, and a target class, attempt to find a suitable constructor.
     *
     * @param args
     * @param target
     * @return
     * @throws NoSuchMethodException
     */
    private static Constructor findCompatibleConstructor(List<Object> args, Class target) throws NoSuchMethodException {
        Constructor retval = null;
        int eligibleCount= 0;

        LOG.info("Target class: {}", target.getName());
        Constructor[] cons = target.getDeclaredConstructors();

        for(Constructor con : cons){
            Class[] paramClasses = con.getParameterTypes();
            if(paramClasses.length == args.size()) {
                LOG.info("found constructor with same number of args..");
                boolean invokable = canInvokeConstructorWithArgs(args, con);
                if(invokable){
                    retval = con;
                    eligibleCount++;
                }
                LOG.info("** invokable --> {}", invokable);
            } else {
                LOG.debug("Skipping constructor with wrong number of arguments.");
            }
        }
        if(eligibleCount > 1){
            LOG.warn("Found multiple invokable constructors for class {}, given arguments {}. Using the last one found.",
                    target, args);
        }
        return retval;
    }


    /**
     * Given a java.util.List of contructor arguments, and a java.lang.Constructor instance, attempt to convert the
     * list to an java.lang.Object array that can be used to invoke the constructor. If a constructor argument needs
     * to be coerced from a List to an Array, do so.
     *
     * @param args
     * @param constructor
     * @return
     */
    private static Object[] getConstructorArgsWithListCoercian(List<Object> args, Constructor constructor){
        Class[] parameterTypes = constructor.getParameterTypes();
        if(parameterTypes.length != args.size()) {
            throw new IllegalArgumentException("Contructor parameter count does not egual argument size.");
        }
        Object[] constructorParams = new Object[args.size()];

        // loop through the arguments, if we hit a list that has to be convered to an array,
        // perform the conversion
        for(int i = 0;i < args.size(); i++){
            Object obj = args.get(i);
            Class paramType = parameterTypes[i];
            Class objectType = obj.getClass();
            LOG.info("Comparing parameter class {} to object class {} to see if assignment is possible.", paramType, objectType);
            if(paramType.equals(objectType)){
                LOG.info("They are the same class.");
                constructorParams[i] = args.get(i);
                continue;
            }
            if(paramType.isAssignableFrom(objectType)){
                LOG.info("Assignment is possible.");
                constructorParams[i] = args.get(i);
                continue;
            }
            if(paramType.isArray() && List.class.isAssignableFrom(objectType)){ // TODO more collection content type checking
                LOG.info("Conversion appears possible...");
                List list = (List)obj;
                LOG.info("Array Type: {}, List type: {}", paramType.getComponentType(), list.get(0).getClass());

                // create an array of the right type
                Object newArrayObj = Array.newInstance(paramType.getComponentType(),list.size());
                for(int j = 0; j < list.size();j++){
                    Array.set(newArrayObj, j, list.get(j));

                }

                constructorParams[i] = newArrayObj;

                LOG.debug("After conversion: {}", constructorParams[i]);
            }
        }
        return constructorParams;
    }

    /**
     * Determine if the given constructor can be invoked with the given arguments List. Consider if
     * list coercian can make it possible.
     * @param args
     * @param constructor
     * @return
     */
    private static boolean canInvokeConstructorWithArgs(List<Object> args, Constructor constructor) {
        Class[] parameterTypes = constructor.getParameterTypes();
        if(parameterTypes.length != args.size()) {
            LOG.warn("parameter types were the wrong size");
            return false;
        }

        for(int i = 0;i < args.size(); i++){
            Object obj = args.get(i);
            Class paramType = parameterTypes[i];
            Class objectType = obj.getClass();
            LOG.info("Comparing parameter class {} to object class {} to see if assignment is possible.", paramType, objectType);
            if(paramType.equals(objectType)){
                LOG.info("Yes, they are the same class.");
                return true;
            }
            if(paramType.isAssignableFrom(objectType)){
                LOG.info("Yes, assignment is possible.");
                return true;
            }
            if(paramType.isArray() && List.class.isAssignableFrom(objectType)){ // TODO more collection content type checking
                LOG.info("I think so. If we convert a List to an array.");
                LOG.info("Array Type: {}, List type: {}", paramType.getComponentType(), ((List)obj).get(0).getClass());

                return true;
            }

            return false;
        }

        return false;
    }



}

