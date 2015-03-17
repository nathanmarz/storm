package org.apache.storm.flux.parser;

import org.apache.storm.flux.model.BoltDef;
import org.apache.storm.flux.model.IncludeDef;
import org.apache.storm.flux.model.SpoutDef;
import org.apache.storm.flux.model.TopologyDef;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yaml.snakeyaml.TypeDescription;
import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.constructor.Constructor;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Map;

public class FluxParser {
    private static final Logger LOG = LoggerFactory.getLogger(FluxParser.class);

    private FluxParser(){}

    public static TopologyDef parseFile(String inputFile, boolean dumpYaml, boolean processIncludes) throws IOException {
        Yaml yaml = yaml();
        FileInputStream in = new FileInputStream(inputFile);
        TopologyDef topology = loadYaml(yaml, in);
        in.close();
        if(dumpYaml){
            dumpYaml(topology, yaml);
        }
        if(processIncludes) {
            return processIncludes(yaml, topology);
        } else {
            return topology;
        }
    }

    public static TopologyDef parseResource(String resource, boolean dumpYaml, boolean processIncludes) throws IOException {
        Yaml yaml = yaml();
        InputStream in = FluxParser.class.getResourceAsStream(resource);
        TopologyDef topology = loadYaml(yaml, in);
        in.close();
        if(dumpYaml){
            dumpYaml(topology, yaml);
        }
        if(processIncludes) {
            return processIncludes(yaml, topology);
        } else {
            return topology;
        }
    }

    private static TopologyDef loadYaml(Yaml yaml, InputStream in){
        return (TopologyDef)yaml.load(in);
    }

    private static void dumpYaml(TopologyDef topology, Yaml yaml){
        System.out.println("Configuration (interpreted): \n" + yaml.dump(topology));
    }

    private static Yaml yaml(){
        Constructor constructor = new Constructor(TopologyDef.class);

        TypeDescription topologyDescription = new TypeDescription(TopologyDef.class);
        topologyDescription.putListPropertyType("spouts", SpoutDef.class);
        topologyDescription.putListPropertyType("bolts", BoltDef.class);
        topologyDescription.putListPropertyType("includes", IncludeDef.class);
        constructor.addTypeDescription(topologyDescription);

        Yaml  yaml = new Yaml(constructor);
        return yaml;
    }

    /**
     *
     * @param yaml the yaml parser for parsing the include file(s)
     * @param topologyDef the topology definition containing (possibly zero) includes
     * @return The TopologyDef with includes resolved.
     */
    private static TopologyDef processIncludes(Yaml yaml, TopologyDef topologyDef) throws IOException {
        //TODO support multiple levels of includes
        if(topologyDef.getIncludes() != null) {
            for (IncludeDef include : topologyDef.getIncludes()){
                TopologyDef includeTopologyDef = null;
                if (include.isResource()) {
                    LOG.info("Loading includes from resource: {}", include.getFile());
                    includeTopologyDef = parseResource(include.getFile(), true, false);
                } else {
                    LOG.info("Loading includes from file: {}", include.getFile());
                    includeTopologyDef = parseFile(include.getFile(), true, false);
                }

                // if overrides are disabled, we won't replace anything that already exists
                boolean override = include.isOverride();
                // name
                if(includeTopologyDef.getName() != null){
                    topologyDef.setName(includeTopologyDef.getName(), override);
                }

                // config
                if(includeTopologyDef.getConfig() != null) {
                    //TODO move this logic to the model class
                    Map<String, Object> config = topologyDef.getConfig();
                    Map<String, Object> includeConfig = includeTopologyDef.getConfig();
                    if(override) {
                        config.putAll(includeTopologyDef.getConfig());
                    } else {
                        for(String key : includeConfig.keySet()){
                            if(config.containsKey(key)){
                                LOG.warn("Ignoring attempt to set topology config property '{}' with override == false", key);
                            }
                            else {
                                config.put(key, includeConfig.get(key));
                            }
                        }
                    }
                }

                //component overrides
                if(includeTopologyDef.getComponents() != null){
                    topologyDef.addAllComponents(includeTopologyDef.getComponents(), override);
                }
                //bolt overrides
                if(includeTopologyDef.getBolts() != null){
                    topologyDef.addAllBolts(includeTopologyDef.getBolts(), override);
                }
                //spout overrides
                if(includeTopologyDef.getSpouts() != null) {
                    topologyDef.addAllSpouts(includeTopologyDef.getSpouts(), override);
                }
                //stream overrides
                //TODO streams should be uniquely identifiable
                if(includeTopologyDef.getStreams() != null) {
                    topologyDef.addAllStreams(includeTopologyDef.getStreams(), override);
                }
            } // end include processing
        }
        return topologyDef;
    }
}
