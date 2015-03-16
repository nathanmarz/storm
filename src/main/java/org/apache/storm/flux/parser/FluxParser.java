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

    public static TopologyDef parseFile(String inputFile, boolean dumpYaml) throws IOException {
        Yaml yaml = yaml();
        FileInputStream in = new FileInputStream(inputFile);
        TopologyDef topology = loadYaml(yaml, in);
        in.close();
        if(dumpYaml){
            dumpYaml(topology, yaml);
        }
        return processIncludes(yaml, topology);
    }

    public static TopologyDef parseResource(String resource, boolean dumpYaml) throws IOException {
        Yaml yaml = yaml();
        InputStream in = FluxParser.class.getResourceAsStream(resource);
        TopologyDef topology = loadYaml(yaml, in);
        in.close();
        if(dumpYaml){
            dumpYaml(topology, yaml);
        }
        return processIncludes(yaml, topology);
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
    private static TopologyDef processIncludes(Yaml yaml, TopologyDef topologyDef){

//        for()
        //TODO load referenced YAML file/resource


        // remove includes from topo def after processing

        return topologyDef;
    }
}
