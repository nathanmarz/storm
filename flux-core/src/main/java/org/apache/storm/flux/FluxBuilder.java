/*
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
package org.apache.storm.flux;

import backtype.storm.Config;
import backtype.storm.generated.StormTopology;
import backtype.storm.grouping.CustomStreamGrouping;
import backtype.storm.topology.*;
import backtype.storm.tuple.Fields;
import backtype.storm.utils.Utils;
import org.apache.storm.flux.api.TopologySource;
import org.apache.storm.flux.model.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.*;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

public class FluxBuilder {
    private static Logger LOG = LoggerFactory.getLogger(FluxBuilder.class);

    /**
     * Given a topology definition, return a populated `backtype.storm.Config` instance.
     *
     * @param topologyDef
     * @return
     */
    public static Config buildConfig(TopologyDef topologyDef) {
        // merge contents of `config` into topology config
        Config conf = new Config();
        conf.putAll(topologyDef.getConfig());
        return conf;
    }

    /**
     * Given a topology definition, return a Storm topology that can be run either locally or remotely.
     *
     * @param context
     * @return
     * @throws IllegalAccessException
     * @throws InstantiationException
     * @throws ClassNotFoundException
     * @throws NoSuchMethodException
     * @throws InvocationTargetException
     */
    public static StormTopology buildTopology(ExecutionContext context) throws IllegalAccessException,
            InstantiationException, ClassNotFoundException, NoSuchMethodException, InvocationTargetException {

        StormTopology topology = null;
        TopologyDef topologyDef = context.getTopologyDef();

        if(!topologyDef.validate()){
            throw new IllegalArgumentException("Invalid topology config. Spouts, bolts and streams cannot be " +
                    "defined in the same configuration as a topologySource.");
        }

        // build components that may be referenced by spouts, bolts, etc.
        // the map will be a String --> Object where the object is a fully
        // constructed class instance
        buildComponents(context);



        if(topologyDef.isDslTopology()) {
            // This is a DSL (YAML, etc.) topology...
            LOG.info("Detected DSL topology...");

            TopologyBuilder builder = new TopologyBuilder();

            // create spouts
            buildSpouts(context, builder);

            // we need to be able to lookup bolts by id, then switch based
            // on whether they are IBasicBolt or IRichBolt instances
            buildBolts(context);

            // process stream definitions
            buildStreamDefinitions(context, builder);

            topology = builder.createTopology();
        } else {
            // user class supplied...
            // this also provides a bridge to Trident...
            LOG.info("A topology source has been specified...");
            ObjectDef def = topologyDef.getTopologySource();
            topology = buildExternalTopology(def, context);
        }
        return topology;
    }

    private static StormTopology buildExternalTopology(ObjectDef def, ExecutionContext context)
            throws ClassNotFoundException, IllegalAccessException, InstantiationException, NoSuchMethodException,
            InvocationTargetException {
        Class clazz = Class.forName(def.getClassName());
        Object topologySource = null;
        if (def.hasConstructorArgs()) {
            LOG.debug("Found constructor arguments in definition: " + def.getConstructorArgs().getClass().getName());
            List<Object> cArgs = resolveReferences(def, context);
            Constructor con = findCompatibleConstructor(cArgs, clazz);
            if (con != null) {
                LOG.debug("Found something seemingly compatible, attempting invocation...");
                topologySource =  con.newInstance(getConstructorArgsWithListCoercian(cArgs, con));
            } else {
                throw new IllegalArgumentException("Couldn't find a suitable TopologySource constructor.");
            }
        } else {
            topologySource = clazz.newInstance();
        }
        applyProperties(def, topologySource, context);

        String methodName = context.getTopologyDef().getTopologySource().getMethodName();//"getTopology";
        Method getTopology = findGetTopologyMethod(topologySource, methodName);
        if(getTopology.getParameterTypes()[0].equals(Config.class)){
            Config config = new Config();
            config.putAll(context.getTopologyDef().getConfig());
            return (StormTopology) getTopology.invoke(topologySource, config);
        } else {
            return (StormTopology) getTopology.invoke(topologySource, context.getTopologyDef().getConfig());
        }
    }

    /**
     * Given a `java.lang.Object` instance and a method name, attempt to find a method that matches the input
     * parameter: `java.util.Map` or `backtype.storm.Config`.
     *
     * @param topologySource object to inspect for the specified method
     * @param methodName name of the method to look for
     * @return
     * @throws NoSuchMethodException
     */
    private static Method findGetTopologyMethod(Object topologySource, String methodName) throws NoSuchMethodException {
        Class clazz = topologySource.getClass();
        Method[] methods =  clazz.getMethods();
        ArrayList<Method> candidates = new ArrayList<Method>();
        for(Method method : methods){
            if(!method.getName().equals(methodName)){
                continue;
            }
            if(!method.getReturnType().equals(StormTopology.class)){
                continue;
            }
            Class[] paramTypes = method.getParameterTypes();
            if(paramTypes.length != 1){
                continue;
            }
            if(paramTypes[0].isAssignableFrom(Map.class) || paramTypes[0].isAssignableFrom(Config.class)){
                candidates.add(method);
            }
        }

        if(candidates.size() == 0){
            throw new IllegalArgumentException("Unable to find method '" + methodName + "' method in class: " + clazz.getName());
        } else if (candidates.size() > 1){
            LOG.warn("Found multiple candidate methods in class '" + clazz.getName() + "'. Using the first one found");
        }

        Method retval = candidates.get(0);
        return retval;
    }

    /**
     * @param context
     * @param builder
     */
    private static void buildStreamDefinitions(ExecutionContext context, TopologyBuilder builder)
            throws ClassNotFoundException, NoSuchMethodException, InvocationTargetException, InstantiationException,
            IllegalAccessException {
        TopologyDef topologyDef = context.getTopologyDef();
        // process stream definitions
        for (StreamDef stream : topologyDef.getStreams()) {
            Object boltObj = context.getBolt(stream.getTo());
            BoltDeclarer declarer = null;
            if (boltObj instanceof IRichBolt) {
                declarer = builder.setBolt(stream.getTo(),
                        (IRichBolt) boltObj,
                        topologyDef.parallelismForBolt(stream.getTo()));
            } else if (boltObj instanceof IBasicBolt) {
                declarer = builder.setBolt(
                        stream.getTo(),
                        (IBasicBolt) boltObj,
                        topologyDef.parallelismForBolt(stream.getTo()));
            } else {
                throw new IllegalArgumentException("Class does not appear to be a bolt: " +
                        boltObj.getClass().getName());
            }

            GroupingDef grouping = stream.getGrouping();
            // if the streamId is defined, use it for the grouping, otherwise assume storm's default stream
            String streamId = (grouping.getStreamId() == null ? Utils.DEFAULT_STREAM_ID : grouping.getStreamId());


            switch (grouping.getType()) {
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
                case CUSTOM:
                    declarer.customGrouping(stream.getFrom(), streamId,
                            buildCustomStreamGrouping(stream.getGrouping().getCustomClass(), context));
                    break;
                default:
                    throw new UnsupportedOperationException("unsupported grouping type: " + grouping);
            }
        }
    }

    private static CustomStreamGrouping buildCustomStreamGrouping(ObjectDef def, ExecutionContext context)
            throws ClassNotFoundException,
            IllegalAccessException, InstantiationException, NoSuchMethodException, InvocationTargetException {
        Class clazz = Class.forName(def.getClassName());
        CustomStreamGrouping grouping = null;
        if (def.hasConstructorArgs()) {
            LOG.debug("Found constructor arguments in definition: " + def.getConstructorArgs().getClass().getName());
            List<Object> cArgs = resolveReferences(def, context);
            Constructor con = findCompatibleConstructor(cArgs, clazz);
            if (con != null) {
                LOG.debug("Found something seemingly compatible, attempting invocation...");
                grouping = (CustomStreamGrouping) con.newInstance(getConstructorArgsWithListCoercian(cArgs, con));
            } else {
                throw new IllegalArgumentException("Couldn't find a suitable StreamGroping constructor.");
            }
        } else {
            grouping = (CustomStreamGrouping) clazz.newInstance();
        }
        applyProperties(def, grouping, context);
        return grouping;
    }


    /**
     * Given a topology definition, resolve and instantiate all components found and return a map
     * keyed by the component id.
     *
     * @param context
     * @return
     * @throws ClassNotFoundException
     * @throws NoSuchMethodException
     * @throws IllegalAccessException
     * @throws InvocationTargetException
     * @throws InstantiationException
     */
    private static void buildComponents(ExecutionContext context) throws ClassNotFoundException, NoSuchMethodException,
            IllegalAccessException, InvocationTargetException, InstantiationException {
        Collection<BeanDef> cDefs = context.getTopologyDef().getComponents();
        if (cDefs != null) {
            for (BeanDef bean : cDefs) {
                Class clazz = Class.forName(bean.getClassName());
                Object obj = null;
                if (bean.hasConstructorArgs()) {
                    LOG.debug("Found constructor arguments in bean definition: " +
                            bean.getConstructorArgs().getClass().getName());
                    List<Object> cArgs = resolveReferences(bean, context);

                    Constructor con = findCompatibleConstructor(cArgs, clazz);
                    if (con != null) {
                        LOG.debug("Found something seemingly compatible, attempting invocation...");
                        obj = con.newInstance(getConstructorArgsWithListCoercian(cArgs, con));
                    } else {
                        throw new IllegalArgumentException("Couldn't find a suitable constructor.");
                    }
                } else {
                    obj = clazz.newInstance();
                }
                context.addComponent(bean.getId(), obj);
                applyProperties(bean, obj, context);
            }
        }
    }

    public static List<Object> resolveReferences(ObjectDef bean, ExecutionContext context) {
        LOG.debug("Checking arguments for references.");
        List<Object> cArgs;
        // resolve references
        if (bean.hasReferences()) {
            cArgs = new ArrayList<Object>();
            for (Object arg : bean.getConstructorArgs()) {
                if (arg instanceof BeanReference) {
                    cArgs.add(context.getComponent(((BeanReference) arg).getId()));
                } else {
                    cArgs.add(arg);
                }
            }
        } else {
            cArgs = bean.getConstructorArgs();
        }
        return cArgs;
    }


    public static void applyProperties(ObjectDef bean, Object instance, ExecutionContext context) throws
            IllegalAccessException, InvocationTargetException {
        List<PropertyDef> props = bean.getProperties();
        Class clazz = instance.getClass();
        if (props != null) {
            for (PropertyDef prop : props) {
                Object value = prop.isReference() ? context.getComponent(prop.getRef()) : prop.getValue();
                Method setter = findSetter(clazz, prop.getName(), value);
                if (setter != null) {
                    LOG.debug("found setter, attempting to invoke");
                    // invoke setter
                    setter.invoke(instance, new Object[]{value});
                } else {
                    // look for a public instance variable
                    LOG.debug("no setter found. Looking for a public instance variable...");
                    Field field = findPublicField(clazz, prop.getName(), value);
                    if (field != null) {
                        field.set(instance, value);
                    }
                }
            }
        }
    }

    public static Field findPublicField(Class clazz, String property, Object arg) {
        Field field = null;
        try {
            field = clazz.getField(property);
        } catch (NoSuchFieldException e) {
            LOG.warn("Could not find setter or public variable for property: " + property, e);
        }
        return field;
    }

    public static Method findSetter(Class clazz, String property, Object arg) {
        String setterName = toSetterName(property);

        Method retval = null;
        Method[] methods = clazz.getMethods();
        for (Method method : methods) {
            if (setterName.equals(method.getName())) {
                LOG.debug("Found setter method: " + method.getName());
                retval = method;
            }
        }
        return retval;
    }

    public static String toSetterName(String name) {
        return "set" + name.substring(0, 1).toUpperCase() + name.substring(1, name.length());
    }

    /**
     * @param context
     * @param builder
     * @throws ClassNotFoundException
     * @throws NoSuchMethodException
     * @throws InvocationTargetException
     * @throws InstantiationException
     * @throws IllegalAccessException
     */
    private static void buildSpouts(ExecutionContext context, TopologyBuilder builder) throws ClassNotFoundException,
            NoSuchMethodException, InvocationTargetException, InstantiationException, IllegalAccessException {
        for (SpoutDef sd : context.getTopologyDef().getSpouts()) {
            IRichSpout spout = buildSpout(sd, context);
            builder.setSpout(sd.getId(), spout, sd.getParallelism());
            context.addSpout(sd.getId(), spout);
        }
    }

    /**
     * Given a spout definition, return a Storm spout implementation by attempting to find a matching constructor
     * in the given spout class. Perform list to array conversion as necessary.
     *
     * @param def
     * @return
     * @throws ClassNotFoundException
     * @throws IllegalAccessException
     * @throws InstantiationException
     * @throws NoSuchMethodException
     * @throws InvocationTargetException
     */
    private static IRichSpout buildSpout(SpoutDef def, ExecutionContext context) throws ClassNotFoundException,
            IllegalAccessException, InstantiationException, NoSuchMethodException, InvocationTargetException {
        Class clazz = Class.forName(def.getClassName());
        IRichSpout spout = null;
        if (def.hasConstructorArgs()) {
            LOG.debug("Found constructor arguments in definition: " + def.getConstructorArgs().getClass().getName());
            List<Object> cArgs = resolveReferences(def, context);
            Constructor con = findCompatibleConstructor(cArgs, clazz);
            if (con != null) {
                LOG.debug("Found something seemingly compatible, attempting invocation...");
                spout = (IRichSpout) con.newInstance(getConstructorArgsWithListCoercian(cArgs, con));
            } else {
                throw new IllegalArgumentException("Couldn't find a suitable Spout constructor.");
            }
        } else {
            spout = (IRichSpout) clazz.newInstance();
        }
        applyProperties(def, spout, context);
        return spout;
    }


    /**
     * Given a list of bolt definitions, build a map of Storm bolts with the bolt definition id as the key.
     * Attempt to coerce the given constructor arguments to a matching bolt constructor as much as possible.
     *
     * @param context
     * @return
     * @throws ClassNotFoundException
     * @throws IllegalAccessException
     * @throws InstantiationException
     * @throws NoSuchMethodException
     * @throws InvocationTargetException
     */
    private static void buildBolts(ExecutionContext context) throws ClassNotFoundException, IllegalAccessException,
            InstantiationException, NoSuchMethodException, InvocationTargetException {
//        Map<String, Object> retval= new HashMap<String, Object>();
        for (BoltDef def : context.getTopologyDef().getBolts()) {
            Class clazz = Class.forName(def.getClassName());
            Object bolt = null;
            LOG.debug("Attempting to instantiate bolt: {}", def.getClassName());
            if (def.hasConstructorArgs()) {
                LOG.debug("Found constructor arguments in definition: " + def.getConstructorArgs().getClass().getName());
                List<Object> cArgs = resolveReferences(def, context);
                Constructor con = findCompatibleConstructor(cArgs, clazz);
                if (con != null) {
                    LOG.debug("Found something seemingly compatible, attempting invocation...");
                    bolt = con.newInstance(getConstructorArgsWithListCoercian(cArgs, con));
                } else {
                    throw new IllegalArgumentException("Couldn't find a suitable Bolt constructor.");
                }
            } else {
                bolt = clazz.newInstance();
            }
            context.addBolt(def.getId(), bolt);
            applyProperties(def, bolt, context);
        }
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
        int eligibleCount = 0;

        LOG.debug("Target class: {}", target.getName());
        Constructor[] cons = target.getDeclaredConstructors();

        for (Constructor con : cons) {
            Class[] paramClasses = con.getParameterTypes();
            if (paramClasses.length == args.size()) {
                LOG.debug("found constructor with same number of args..");
                boolean invokable = canInvokeConstructorWithArgs(args, con);
                if (invokable) {
                    retval = con;
                    eligibleCount++;
                }
                LOG.debug("** invokable --> {}", invokable);
            } else {
                LOG.debug("Skipping constructor with wrong number of arguments.");
            }
        }
        if (eligibleCount > 1) {
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
    private static Object[] getConstructorArgsWithListCoercian(List<Object> args, Constructor constructor) {
        Class[] parameterTypes = constructor.getParameterTypes();
        if (parameterTypes.length != args.size()) {
            throw new IllegalArgumentException("Contructor parameter count does not egual argument size.");
        }
        Object[] constructorParams = new Object[args.size()];

        // loop through the arguments, if we hit a list that has to be convered to an array,
        // perform the conversion
        for (int i = 0; i < args.size(); i++) {
            Object obj = args.get(i);
            Class paramType = parameterTypes[i];
            Class objectType = obj.getClass();
            LOG.debug("Comparing parameter class {} to object class {} to see if assignment is possible.",
                    paramType, objectType);
            if (paramType.equals(objectType)) {
                LOG.debug("They are the same class.");
                constructorParams[i] = args.get(i);
                continue;
            }
            if (paramType.isAssignableFrom(objectType)) {
                LOG.debug("Assignment is possible.");
                constructorParams[i] = args.get(i);
                continue;
            }
            if(isPrimitiveNumber(paramType) && Number.class.isAssignableFrom(objectType)){
                constructorParams[i] = args.get(i);
                continue;
            }

            if (paramType.isArray() && List.class.isAssignableFrom(objectType)) {
                // TODO more collection content type checking
                LOG.debug("Conversion appears possible...");
                List list = (List) obj;
                LOG.debug("Array Type: {}, List type: {}", paramType.getComponentType(), list.get(0).getClass());

                // create an array of the right type
                Object newArrayObj = Array.newInstance(paramType.getComponentType(), list.size());
                for (int j = 0; j < list.size(); j++) {
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
     *
     * @param args
     * @param constructor
     * @return
     */
    private static boolean canInvokeConstructorWithArgs(List<Object> args, Constructor constructor) {
        Class[] parameterTypes = constructor.getParameterTypes();
        if (parameterTypes.length != args.size()) {
            LOG.warn("parameter types were the wrong size");
            return false;
        }

        for (int i = 0; i < args.size(); i++) {
            Object obj = args.get(i);
            Class paramType = parameterTypes[i];
            Class objectType = obj.getClass();
            LOG.debug("Comparing parameter class {} to object class {} to see if assignment is possible.",
                    paramType, objectType);
            if (paramType.equals(objectType)) {
                LOG.debug("Yes, they are the same class.");
                return true;
            }
            if (paramType.isAssignableFrom(objectType)) {
                LOG.debug("Yes, assignment is possible.");
                return true;
            }
            if(isPrimitiveNumber(paramType) && Number.class.isAssignableFrom(objectType)){
                return true;
            }
            if (paramType.isArray() && List.class.isAssignableFrom(objectType)) {
                // TODO more collection content type checking
                LOG.debug("I think so. If we convert a List to an array.");
                LOG.debug("Array Type: {}, List type: {}", paramType.getComponentType(), ((List) obj).get(0).getClass());

                return true;
            }
            return false;
        }
        return false;
    }

    public static boolean isPrimitiveNumber(Class clazz){
        return clazz.isPrimitive() && !clazz.equals(boolean.class);
    }
}

