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
            for (IncludeDef include : topologyDef.getIncludes()) {
                //TODO load referenced YAML file/resource
                TopologyDef td = null;
                if (include.isResource()) {
                    LOG.info("Loading includes from resource: {}", include.getFile());
                    td = parseResource(include.getFile(), true, false);
                } else {
                    LOG.info("Loading includes from file: {}", include.getFile());
                    td = parseFile(include.getFile(), true, false);
                }
                boolean override = include.isOverride();
                // name
                if(td.getName() != null){
                    topologyDef.setName(td.getName(), override);
                }

                // config
                //TODO deal with config overrides
                if(td.getConfig() != null) {
                    topologyDef.getConfig().putAll(td.getConfig());
                }

                //TODO deal with component overrides
                if(td.getComponents() != null){
                    topologyDef.addAllComponents(td.getComponents(), override);
                }

                //TODO deal with bolt overrides
                if(td.getBolts() != null){
                    topologyDef.addAllBolts(td.getBolts(), override);
                }

                //TODO deal with spout overrides
                if(td.getSpouts() != null) {
                    topologyDef.addAllSpouts(td.getSpouts(), override);
                }

                //TODO deal with stream overrides
                //TODO streams should be uniquely identifiable
                if(td.getStreams() != null) {
                    topologyDef.addAllStreams(td.getStreams(), override);
                }
            }
        }


        // remove includes from topo def after processing

        return topologyDef;
    }
}
